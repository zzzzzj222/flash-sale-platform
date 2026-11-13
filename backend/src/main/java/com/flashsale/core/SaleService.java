package com.flashsale.core;

import java.time.Instant;
import java.util.List;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import com.flashsale.SaleProperties;
import com.flashsale.api.ApiException;
import com.flashsale.api.Contracts.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.dao.DataAccessException;
import tools.jackson.databind.ObjectMapper;

@Service
public class SaleService {
    public static final int MAX_BATCH_SIZE = 16;
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SaleService.class);
    private final SaleStore store;
    private final SaleProperties properties;
    private final ObjectMapper json;
    private final Preallocation preallocation;

    /**
     * 初始化数据访问、销售配置和事件 JSON 序列化组件。
     */
    public SaleService(SaleStore store, SaleProperties properties, ObjectMapper json, Preallocation preallocation) {
        this.store = store;
        this.properties = properties;
        this.json = json;
        this.preallocation = preallocation;
    }

    /**
     * 校验活动时间和单用户限购量，在事务中创建租户活动并返回活动详情。
     * 初始可用库存等于活动总库存，校验失败时抛出参数错误异常。
     */
    @Transactional
    public ActivityView create(String tenant, CreateActivity input) {
        long now = store.now();
        if (!input.endsAt().isAfter(input.startsAt()) || input.endsAt().toEpochMilli() <= now
                || input.limitPerUser() > input.stock()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_ACTIVITY", "活动结束时间必须晚于开始时间和当前时间，限购量不能超过配额");
        }
        String id = UUID.randomUUID().toString();
        store.jdbc().sql("""
                INSERT INTO activities (tenant_id,id,resource_id,title,total_stock,available_stock,limit_per_user,
                hold_seconds,queue_timeout_seconds,starts_at_ms,ends_at_ms,created_at_ms)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """).params(tenant, id, input.resourceId(), input.title(), input.stock(), input.stock(),
                input.limitPerUser(), input.holdSeconds(), input.queueTimeoutSeconds(), input.startsAt().toEpochMilli(),
                input.endsAt().toEpochMilli(), now).update();
        return store.activity(tenant, id, false);
    }

    /**
     * 按创建时间倒序返回租户最近创建的最多 100 个活动。
     */
    public List<ActivityView> activities(String tenant) {
        return store.activities(tenant);
    }

    /**
     * 查询指定租户的活动详情，活动不存在时抛出资源未找到异常。
     */
    public ActivityView activity(String tenant, String id) { return store.activity(tenant, id, false); }

    /**
     * 在 Redis 原子校验库存、限购和请求编号，保存预扣结果及待投递记录。
     * MySQL 申请与库存由消息消费者异步落库。
     */
    public ReservationView submit(String tenant, Reserve input) {
        return preallocation.submit(tenant, input);
    }

    /**
     * 校验同一请求编号对应的活动、用户和数量是否一致，不一致时抛出幂等冲突异常。
     */
    private void checkSameRequest(ReservationView previous, Reserve input) {
        if (!previous.activityId().equals(input.activityId().toString())
                || !previous.userId().equals(input.userId()) || previous.quantity() != input.quantity()) {
            throw ApiException.conflict("IDEMPOTENCY_CONFLICT", "原请求编号已关联其他参数，请核对请求内容");
        }
    }

    /**
     * 在事务中锁定排队请求及活动，检查时效、用户额度和库存后尝试建立限时预占。
     * 成功时扣减库存并占用用户额度，失败时记录拒绝原因，状态变化时登记通知事件。
     * 对已离开排队状态的请求直接返回。
     */
    @Transactional
    public void process(String tenant, String requestId) {
        var pending = preallocation.find(tenant, requestId);
        var message = pending.filter(view -> view.state().equals("QUEUED") || view.state().equals("REJECTED"))
                .map(view -> new RequestMessage(tenant, requestId, view))
                .orElseGet(() -> new RequestMessage(tenant, requestId));
        process(message);
    }

    @Transactional
    public void process(RequestMessage message) {
        if (message.reservation() == null) {
            var activity = lockActivity(message.tenantId(), message.requestId());
            processStored(message.tenantId(), activity,
                    List.of(store.reservation(message.tenantId(), message.requestId(), true)));
            return;
        }
        processBatch(List.of(message));
    }

    /**
     * 同一租户、活动最多 16 个申请共用一个事务，复用活动锁并批量写入申请、库存、额度和事件。
     */
    @Transactional
    public void processBatch(List<RequestMessage> messages) {
        if (messages.isEmpty() || messages.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("Invalid reservation batch size");
        }
        var first = messages.getFirst();
        var inputs = new LinkedHashMap<String, ReservationView>();
        for (var message : messages) {
            validateMessage(message);
            if (!first.tenantId().equals(message.tenantId())
                    || !first.reservation().activityId().equals(message.reservation().activityId())) {
                throw new IllegalArgumentException("Reservation batch must belong to one tenant and activity");
            }
            var previous = inputs.putIfAbsent(message.requestId(), message.reservation());
            if (previous != null) { checkSameRequest(previous, reserveInput(message.reservation())); }
        }
        // 外键检查会取得活动共享锁，先取得排他锁以避免并发插入后的锁升级死锁。
        var activity = store.activity(first.tenantId(), first.reservation().activityId(), true);
        processStored(first.tenantId(), activity, persistRequests(first.tenantId(), List.copyOf(inputs.values())));
    }

    /** 校验批次中的申请快照编号与可持久化状态。 */
    private void validateMessage(RequestMessage message) {
        var input = message.reservation();
        if (input == null || !message.requestId().equals(input.requestId())
                || !(input.state().equals("QUEUED") || input.state().equals("REJECTED"))) {
            throw new IllegalArgumentException("Invalid pending reservation message");
        }
    }

    /** 复用原有幂等参数检查所需的请求结构。 */
    private Reserve reserveInput(ReservationView input) {
        return new Reserve(UUID.fromString(input.requestId()), UUID.fromString(input.activityId()), input.userId(), input.quantity());
    }

    /** 批量补齐申请后只执行一次当前读，保留已有申请状态并逐条检查幂等参数。 */
    private List<ReservationView> persistRequests(String tenant, List<ReservationView> inputs) {
        var parameters = new ArrayList<Object>();
        for (var input : inputs) {
            Collections.addAll(parameters, tenant, input.requestId(), input.activityId(), input.userId(), input.quantity(),
                    input.state(), input.reason(), input.queueDeadline().toEpochMilli(), input.createdAt().toEpochMilli(),
                    input.updatedAt().toEpochMilli());
        }
        store.jdbc().sql("""
                INSERT INTO reservations (tenant_id,request_id,activity_id,user_id,quantity,state,reason,
                queue_deadline_ms,created_at_ms,updated_at_ms) VALUES
                """ + String.join(",", Collections.nCopies(inputs.size(), "(?,?,?,?,?,?,?,?,?,?)"))
                + " ON DUPLICATE KEY UPDATE request_id=request_id").params(parameters).update();
        var requests = store.jdbc().sql("""
                SELECT * FROM reservations WHERE tenant_id=:tenant AND request_id IN (:ids)
                ORDER BY request_id FOR UPDATE
                """).param("tenant", tenant).param("ids", inputs.stream().map(ReservationView::requestId).toList())
                .query(SaleStore::reservationRow).list().stream()
                .collect(java.util.stream.Collectors.toMap(ReservationView::requestId, request -> request));
        return inputs.stream().map(input -> {
            var previous = requests.get(input.requestId());
            checkSameRequest(previous, reserveInput(input));
            return previous;
        }).toList();
    }

    /** 按批次中的申请顺序决定结果，锁内共享库存和额度视图，最终一次提交所有变更。 */
    private void processStored(String tenant, ActivityView activity, List<ReservationView> requests) {
        var terminal = requests.stream().filter(request -> !request.state().equals("QUEUED")).toList();
        var synchronizedIds = preallocation.synchronizedRequests(tenant, terminal);
        if (synchronizedIds.size() == requests.size()) { return; }
        long now = store.now();
        var users = requests.stream().filter(request -> request.state().equals("QUEUED")
                && now < request.queueDeadline().toEpochMilli() && now < activity.endsAt().toEpochMilli()
                && now >= activity.startsAt().toEpochMilli()).map(ReservationView::userId).distinct().sorted().toList();
        var quotas = loadQuotas(tenant, activity.id(), users);
        var changes = new ArrayList<ReservationView>();
        var results = new ArrayList<ReservationView>();
        var changedUsers = new java.util.HashSet<String>();
        int remainingStock = activity.availableStock();
        for (var request : requests) {
            if (!request.state().equals("QUEUED")) {
                if (!synchronizedIds.contains(request.requestId())) { results.add(request); }
                continue;
            }
            String reason = null;
            if (now >= request.queueDeadline().toEpochMilli() || now >= activity.endsAt().toEpochMilli()) {
                reason = "QUEUE_TIMEOUT";
            } else if (now < activity.startsAt().toEpochMilli()) {
                reason = "ACTIVITY_CLOSED";
            } else if (quotas.get(request.userId()) + request.quantity() > activity.limitPerUser()) {
                reason = "LIMIT_EXCEEDED";
            } else if (remainingStock < request.quantity()) {
                reason = "SOLD_OUT";
            } else {
                remainingStock -= request.quantity();
                quotas.merge(request.userId(), request.quantity(), Integer::sum);
                changedUsers.add(request.userId());
            }
            var result = new ReservationView(request.requestId(), request.activityId(), request.userId(), request.quantity(),
                    reason == null ? "RESERVED" : "REJECTED", reason, request.orderId(), request.queueDeadline(),
                    reason == null ? Instant.ofEpochMilli(now + activity.holdSeconds() * 1000L) : request.expiresAt(),
                    request.createdAt(), Instant.ofEpochMilli(now));
            changes.add(result);
            results.add(result);
        }
        int reserved = activity.availableStock() - remainingStock;
        if (reserved > 0) {
            int changed = store.jdbc().sql("""
                UPDATE activities SET available_stock=available_stock-?
                WHERE tenant_id=? AND id=? AND available_stock>=?
                """).params(reserved, tenant, activity.id(), reserved).update();
            if (changed != 1) { throw new IllegalStateException("Reservation stock invariant violated"); }
            updateQuotas(tenant, activity.id(), quotas, changedUsers);
        }
        updateRequests(tenant, changes);
        notifyResults(tenant, results, now);
    }

    /** 批量创建缺失额度行后按用户顺序加锁，返回本批共用的额度视图。 */
    private Map<String, Integer> loadQuotas(String tenant, String activityId, List<String> users) {
        var quotas = new HashMap<String, Integer>();
        if (users.isEmpty()) { return quotas; }
        var parameters = new ArrayList<Object>();
        for (var user : users) { Collections.addAll(parameters, tenant, activityId, user); }
        store.jdbc().sql("INSERT INTO user_quotas (tenant_id,activity_id,user_id,used_quantity) VALUES "
                + String.join(",", Collections.nCopies(users.size(), "(?,?,?,0)"))
                + " ON DUPLICATE KEY UPDATE used_quantity=used_quantity").params(parameters).update();
        store.jdbc().sql("""
                SELECT user_id,used_quantity FROM user_quotas
                WHERE tenant_id=:tenant AND activity_id=:activity AND user_id IN (:users) ORDER BY user_id FOR UPDATE
                """).param("tenant", tenant).param("activity", activityId).param("users", users)
                .query((row, index) -> {
                    quotas.put(row.getString(1), row.getInt(2));
                    return row.getString(1);
                }).list();
        return quotas;
    }

    /** 在活动与额度行锁保护下，用一条更新写入本批成功申请的累计额度。 */
    private void updateQuotas(String tenant, String activityId, Map<String, Integer> quotas, Set<String> users) {
        var ordered = users.stream().sorted().toList();
        var clauses = new ArrayList<String>();
        for (int index = 0; index < ordered.size(); index++) { clauses.add("WHEN :user" + index + " THEN :used" + index); }
        var update = store.jdbc().sql("UPDATE user_quotas SET used_quantity=CASE user_id " + String.join(" ", clauses)
                + " END WHERE tenant_id=:tenant AND activity_id=:activity AND user_id IN (:users)")
                .param("tenant", tenant).param("activity", activityId).param("users", ordered);
        for (int index = 0; index < ordered.size(); index++) {
            update.param("user" + index, ordered.get(index)).param("used" + index, quotas.get(ordered.get(index)));
        }
        if (update.update() != ordered.size()) { throw new IllegalStateException("Reservation quota invariant violated"); }
    }

    /** 批量写入已锁定申请的新状态，幂等参数与关联订单保持当前数据库值。 */
    private void updateRequests(String tenant, List<ReservationView> requests) {
        if (requests.isEmpty()) { return; }
        var parameters = new ArrayList<Object>();
        for (var request : requests) {
            Collections.addAll(parameters, tenant, request.requestId(), request.activityId(), request.userId(), request.quantity(),
                    request.state(), request.reason(), request.orderId(), request.queueDeadline().toEpochMilli(),
                    request.expiresAt() == null ? null : request.expiresAt().toEpochMilli(), request.createdAt().toEpochMilli(),
                    request.updatedAt().toEpochMilli());
        }
        store.jdbc().sql("""
                INSERT INTO reservations (tenant_id,request_id,activity_id,user_id,quantity,state,reason,order_id,
                queue_deadline_ms,expires_at_ms,created_at_ms,updated_at_ms) VALUES
                """ + String.join(",", Collections.nCopies(requests.size(), "(?,?,?,?,?,?,?,?,?,?,?,?)"))
                + " AS incoming ON DUPLICATE KEY UPDATE state=incoming.state,reason=incoming.reason,"
                + "expires_at_ms=incoming.expires_at_ms,updated_at_ms=incoming.updated_at_ms").params(parameters).update();
    }

    /**
     * 查询指定租户的预占处理结果，请求不存在时抛出资源未找到异常。
     */
    public ReservationView result(String tenant, String id) {
        var stored = store.find(tenant, id, false);
        if (stored.isPresent()) {
            if (!stored.get().state().equals("QUEUED")) { preallocation.synchronizeQuietly(tenant, stored.get()); }
            return stored.get();
        }
        return preallocation.find(tenant, id).orElseThrow(ApiException::notFound);
    }

    /**
     * 按创建时间倒序返回指定租户内用户最近的最多 30 条预占请求。
     */
    public List<ReservationView> history(String tenant, String user) {
        var stored = store.jdbc().sql("SELECT * FROM reservations WHERE tenant_id=? AND user_id=? ORDER BY created_at_ms DESC LIMIT 30")
                .params(tenant, user).query(SaleStore::reservationRow).list();
        var merged = new HashMap<String, ReservationView>();
        try {
            for (var pending : preallocation.history(tenant, user)) { merged.put(pending.requestId(), pending); }
        } catch (DataAccessException error) {
            return stored;
        }
        for (var request : stored) { merged.put(request.requestId(), request); }
        return merged.values().stream().sorted(Comparator.comparing(ReservationView::createdAt).reversed()).limit(30).toList();
    }

    /**
     * 将有效预占确认为成交并绑定订单，同一订单重复确认时返回原记录。
     * 预占已超时时释放库存及用户额度并返回过期记录，订单冲突或状态不允许时抛出异常。
     */
    @Transactional
    public ReservationView confirm(String tenant, String id, String orderId) {
        lockActivity(tenant, id);
        var request = store.reservation(tenant, id, true);
        if (request.state().equals("CONFIRMED")) {
            if (!orderId.equals(request.orderId())) {
                throw ApiException.conflict("ORDER_CONFLICT", "该预占已关联另一笔订单");
            }
            return request;
        }
        if (!request.state().equals("RESERVED")) {
            throw ApiException.conflict("INVALID_STATE", "当前状态无法确认成交");
        }
        long now = store.now();
        if (request.expiresAt().toEpochMilli() <= now) {
            return releaseHeld(tenant, request, "EXPIRED", now);
        }
        store.jdbc().sql("UPDATE reservations SET state='CONFIRMED',order_id=?,updated_at_ms=? WHERE tenant_id=? AND request_id=?")
                .params(orderId, now, tenant, id).update();
        var result = store.reservation(tenant, id, false);
        notifyResult(tenant, result, now);
        return result;
    }

    /**
     * 取消排队或释放预占，释放预占时归还库存和用户额度，已超时的预占标记为过期。
     * 已释放或已过期的请求直接返回，其他不支持取消的状态抛出冲突异常。
     */
    @Transactional
    public ReservationView release(String tenant, String id) {
        var request = lockPendingRequest(tenant, id);
        long now = store.now();
        return switch (request.state()) {
            case "RESERVED" -> releaseHeld(tenant, request,
                    request.expiresAt().toEpochMilli() <= now ? "EXPIRED" : "RELEASED", now);
            case "QUEUED" -> finish(tenant, id, "RELEASED", "USER_CANCELLED", now);
            case "RELEASED", "EXPIRED" -> request;
            default -> throw ApiException.conflict("INVALID_STATE", "当前状态无法取消预占");
        };
    }

    /**
     * 锁定请求并按数据库时间处理超时，预占超时则归还资源，排队超时则记录拒绝原因。
     * 未超时或已进入其他状态的请求无需处理。
     */
    @Transactional
    public void expire(String tenant, String id) {
        var request = lockPendingRequest(tenant, id);
        long now = store.now();
        if (request.state().equals("RESERVED") && request.expiresAt().toEpochMilli() <= now) {
            releaseHeld(tenant, request, "EXPIRED", now);
        } else if (request.state().equals("QUEUED") && request.queueDeadline().toEpochMilli() <= now) {
            finish(tenant, id, "REJECTED", "QUEUE_TIMEOUT", now);
        }
    }

    /**
     * 在调用方事务及活动、预占行锁保护下归还库存和用户额度，再更新请求并登记通知。
     * 用户额度未能按预占数量扣回时抛出异常，使事务回滚。
     *
     * @param state 释放后的目标状态，由调用方传入 RELEASED 或 EXPIRED
     * @param now 本次操作使用的数据库毫秒时间戳
     */
    private ReservationView releaseHeld(String tenant, ReservationView request, String state, long now) {
        store.jdbc().sql("UPDATE activities SET available_stock=available_stock+? WHERE tenant_id=? AND id=?")
                .params(request.quantity(), tenant, request.activityId()).update();
        int quotaChanged = store.jdbc().sql("""
                UPDATE user_quotas SET used_quantity=used_quantity-?
                WHERE tenant_id=? AND activity_id=? AND user_id=? AND used_quantity>=?
                """).params(request.quantity(), tenant, request.activityId(), request.userId(), request.quantity()).update();
        if (quotaChanged != 1) { throw new IllegalStateException("Reservation quota invariant violated"); }
        return finish(tenant, request.requestId(), state, state.equals("EXPIRED") ? "HOLD_TIMEOUT" : "USER_CANCELLED", now);
    }

    /**
     * 按不可变的活动编号先锁活动，调用方随后锁定申请并重新读取当前状态，统一业务事务的加锁顺序。
     */
    private ActivityView lockActivity(String tenant, String requestId) {
        var request = store.reservation(tenant, requestId, false);
        return store.activity(tenant, request.activityId(), true);
    }

    /**
     * 补齐尚未落库的申请，按活动、申请顺序加锁，并用当前读取得并发消费后的最新状态。
     */
    private ReservationView lockPendingRequest(String tenant, String id) {
        var stored = store.find(tenant, id, false);
        var request = stored.orElseGet(() -> preallocation.find(tenant, id).orElseThrow(ApiException::notFound));
        store.activity(tenant, request.activityId(), true);
        if (stored.isEmpty() && (request.state().equals("QUEUED") || request.state().equals("REJECTED"))) {
            validateMessage(new RequestMessage(tenant, id, request));
            return persistRequests(tenant, List.of(request)).getFirst();
        }
        // 消费者可能已提交并同步 Redis，旧事务快照仍看不到申请，需要当前读取得最新状态。
        return store.reservation(tenant, id, true);
    }

    /**
     * 在调用方事务中更新请求状态、原因和更新时间，登记通知事件并返回最新记录。
     */
    private ReservationView finish(String tenant, String id, String state, String reason, long now) {
        store.jdbc().sql("UPDATE reservations SET state=?,reason=?,updated_at_ms=? WHERE tenant_id=? AND request_id=?")
                .params(state, reason, now, tenant, id).update();
        var result = store.reservation(tenant, id, false);
        notifyResult(tenant, result, now);
        return result;
    }

    /**
     * 在事务内登记 Redis 台账同步任务和可选 Webhook，提交后立即尝试同步 Redis。
     */
    private void notifyResult(String tenant, ReservationView result, long now) {
        notifyResults(tenant, List.of(result), now);
    }

    /** 结果与补偿事件同事务批量落库，提交后用一次 Lua 尝试同步本批结果。 */
    private void notifyResults(String tenant, List<ReservationView> results, long now) {
        if (results.isEmpty()) { return; }
        var parameters = new ArrayList<Object>();
        int events = 0;
        for (var result : results) {
            Collections.addAll(parameters, UUID.randomUUID().toString(), tenant, result.requestId(), "REDIS",
                    "REDIS_" + result.state(), json.writeValueAsString(result), now, now);
            events++;
            if (properties.client(tenant).hasWebhook()) {
                String eventId = UUID.randomUUID().toString();
                var event = new BusinessEvent(eventId, "reservation." + result.state().toLowerCase(java.util.Locale.ROOT),
                        Instant.ofEpochMilli(now), result);
                Collections.addAll(parameters, eventId, tenant, result.requestId(), "WEBHOOK", result.state(),
                        json.writeValueAsString(event), now, now);
                events++;
            }
        }
        store.jdbc().sql("""
                INSERT INTO outbox (id,tenant_id,request_id,destination,event_key,payload,available_at_ms,created_at_ms)
                VALUES
                """ + String.join(",", Collections.nCopies(events, "(?,?,?,?,?,?,?,?)"))
                + " ON DUPLICATE KEY UPDATE id=id").params(parameters).update();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { preallocation.synchronizeMissing(tenant, results); }
                catch (RuntimeException error) {
                    // 业务事务已提交，持久化 Outbox 负责恢复，避免将提交成功误报为消费失败。
                    log.warn("Redis result batch synchronization deferred for {}", tenant);
                }
            }
        });
    }
}
