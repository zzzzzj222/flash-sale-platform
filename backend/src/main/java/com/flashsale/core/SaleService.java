package com.flashsale.core;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import com.flashsale.SaleProperties;
import com.flashsale.api.ApiException;
import com.flashsale.api.Contracts.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
public class SaleService {
    private final SaleStore store;
    private final SaleProperties properties;
    private final ObjectMapper json;

    /**
     * 初始化数据访问、销售配置和事件 JSON 序列化组件。
     */
    public SaleService(SaleStore store, SaleProperties properties, ObjectMapper json) {
        this.store = store;
        this.properties = properties;
        this.json = json;
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
     * 幂等提交预占请求，校验活动时间和申请数量，并在同一事务中保存排队记录及消息事件。
     * 已有请求编号且参数一致时返回原记录，参数不一致时抛出幂等冲突异常。
     */
    @Transactional
    public ReservationView submit(String tenant, Reserve input) {
        String id = input.requestId().toString();
        var previous = store.find(tenant, id, false);
        if (previous.isPresent()) {
            checkSameRequest(previous.get(), input);
            return previous.get();
        }
        var activity = store.activity(tenant, input.activityId().toString(), false);
        long now = store.now();
        if (now < activity.startsAt().toEpochMilli() || now >= activity.endsAt().toEpochMilli()) {
            throw ApiException.conflict("ACTIVITY_CLOSED", "活动尚未开始或已经结束");
        }
        if (input.quantity() > activity.limitPerUser()) {
            throw ApiException.conflict("LIMIT_EXCEEDED", "本次数量超过每人限购量");
        }
        long deadline = Math.min(activity.endsAt().toEpochMilli(), now + activity.queueTimeoutSeconds() * 1000L);
        store.jdbc().sql("""
                INSERT INTO reservations (tenant_id,request_id,activity_id,user_id,quantity,state,
                queue_deadline_ms,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,'QUEUED',?,?,?)
                ON DUPLICATE KEY UPDATE request_id=request_id
                """).params(tenant, id, activity.id(), input.userId(), input.quantity(), deadline, now, now).update();
        var result = store.reservation(tenant, id, true);
        checkSameRequest(result, input);
        if (result.state().equals("QUEUED")) {
            enqueue(tenant, id, "RABBIT", "REQUESTED", new RequestMessage(tenant, id), now);
        }
        return result;
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
        var request = store.reservation(tenant, requestId, true);
        if (!request.state().equals("QUEUED")) { return; }
        // shortcut: one activity row serializes stock; consider quota partitioning after hotspot load tests.
        var activity = store.activity(tenant, request.activityId(), true);
        long now = store.now();
        if (now >= request.queueDeadline().toEpochMilli() || now >= activity.endsAt().toEpochMilli()) {
            finish(tenant, requestId, "REJECTED", "QUEUE_TIMEOUT", now);
            return;
        }
        if (now < activity.startsAt().toEpochMilli()) {
            finish(tenant, requestId, "REJECTED", "ACTIVITY_CLOSED", now);
            return;
        }
        store.jdbc().sql("""
                INSERT INTO user_quotas (tenant_id,activity_id,user_id,used_quantity) VALUES (?,?,?,0)
                ON DUPLICATE KEY UPDATE used_quantity=used_quantity
                """).params(tenant, activity.id(), request.userId()).update();
        int used = store.jdbc().sql("SELECT used_quantity FROM user_quotas WHERE tenant_id=? AND activity_id=? AND user_id=? FOR UPDATE")
                .params(tenant, activity.id(), request.userId()).query(Integer.class).single();
        if (used + request.quantity() > activity.limitPerUser()) {
            finish(tenant, requestId, "REJECTED", "LIMIT_EXCEEDED", now);
            return;
        }
        int changed = store.jdbc().sql("""
                UPDATE activities SET available_stock=available_stock-?
                WHERE tenant_id=? AND id=? AND available_stock>=?
                """).params(request.quantity(), tenant, activity.id(), request.quantity()).update();
        if (changed == 0) {
            finish(tenant, requestId, "REJECTED", "SOLD_OUT", now);
            return;
        }
        store.jdbc().sql("UPDATE user_quotas SET used_quantity=used_quantity+? WHERE tenant_id=? AND activity_id=? AND user_id=?")
                .params(request.quantity(), tenant, activity.id(), request.userId()).update();
        store.jdbc().sql("UPDATE reservations SET state='RESERVED',expires_at_ms=?,updated_at_ms=? WHERE tenant_id=? AND request_id=?")
                .params(now + activity.holdSeconds() * 1000L, now, tenant, requestId).update();
        notifyResult(tenant, store.reservation(tenant, requestId, false), now);
    }

    /**
     * 查询指定租户的预占处理结果，请求不存在时抛出资源未找到异常。
     */
    public ReservationView result(String tenant, String id) { return store.reservation(tenant, id, false); }

    /**
     * 按创建时间倒序返回指定租户内用户最近的最多 30 条预占请求。
     */
    public List<ReservationView> history(String tenant, String user) {
        return store.jdbc().sql("SELECT * FROM reservations WHERE tenant_id=? AND user_id=? ORDER BY created_at_ms DESC LIMIT 30")
                .params(tenant, user).query(SaleStore::reservationRow).list();
    }

    /**
     * 将有效预占确认为成交并绑定订单，同一订单重复确认时返回原记录。
     * 预占已超时时释放库存及用户额度并返回过期记录，订单冲突或状态不允许时抛出异常。
     */
    @Transactional
    public ReservationView confirm(String tenant, String id, String orderId) {
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
        var request = store.reservation(tenant, id, true);
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
        var request = store.reservation(tenant, id, true);
        long now = store.now();
        if (request.state().equals("RESERVED") && request.expiresAt().toEpochMilli() <= now) {
            releaseHeld(tenant, request, "EXPIRED", now);
        } else if (request.state().equals("QUEUED") && request.queueDeadline().toEpochMilli() <= now) {
            finish(tenant, id, "REJECTED", "QUEUE_TIMEOUT", now);
        }
    }

    /**
     * 在调用方事务及预占行锁保护下锁定活动，归还库存和用户额度，再更新请求并登记通知。
     * 用户额度未能按预占数量扣回时抛出异常，使事务回滚。
     *
     * @param state 释放后的目标状态，由调用方传入 RELEASED 或 EXPIRED
     * @param now 本次操作使用的数据库毫秒时间戳
     */
    private ReservationView releaseHeld(String tenant, ReservationView request, String state, long now) {
        store.activity(tenant, request.activityId(), true);
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
     * 为已配置 Webhook 的租户构造预占状态事件，并写入 outbox 等待异步投递。
     */
    private void notifyResult(String tenant, ReservationView result, long now) {
        if (properties.client(tenant).hasWebhook()) {
            String eventId = UUID.randomUUID().toString();
            var event = new BusinessEvent(eventId, "reservation." + result.state().toLowerCase(java.util.Locale.ROOT),
                    Instant.ofEpochMilli(now), result);
            insertEvent(eventId, tenant, result.requestId(), "WEBHOOK", result.state(), json.writeValueAsString(event), now);
        }
    }

    /**
     * 生成事件编号并将消息体序列化为 JSON，写入 outbox 等待异步发送。
     *
     * @param destination 消息投递目标，当前排队请求使用 RABBIT
     * @param key 事件类型标识，与租户和请求编号共同用于事件去重
     */
    private void enqueue(String tenant, String id, String destination, String key, Object payload, long now) {
        insertEvent(UUID.randomUUID().toString(), tenant, id, destination, key, json.writeValueAsString(payload), now);
    }

    /**
     * 在调用方事务中插入 outbox 事件，使业务数据与待发送事件一同提交。
     * 唯一键冲突时保留已有事件，新事件的可投递时间设为传入的当前时间。
     */
    private void insertEvent(String eventId, String tenant, String id, String destination, String key, String payload, long now) {
        store.jdbc().sql("""
                INSERT INTO outbox (id,tenant_id,request_id,destination,event_key,payload,available_at_ms,created_at_ms)
                VALUES (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE id=id
                """).params(eventId, tenant, id, destination, key, payload, now, now).update();
    }
}
