package com.flashsale.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import com.flashsale.api.ApiException;
import com.flashsale.api.Contracts.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

/** Redis 持久库存台账，原子预扣并保留申请直到数据库结果同步完成。 */
@Component
public class Preallocation {
    private static final Logger log = LoggerFactory.getLogger(Preallocation.class);
    private static final DefaultRedisScript<String> RESERVE = new DefaultRedisScript<>();
    private static final DefaultRedisScript<Long> SYNC = new DefaultRedisScript<>();
    static {
        RESERVE.setLocation(new ClassPathResource("redis/preallocate.lua"));
        RESERVE.setResultType(String.class);
        SYNC.setLocation(new ClassPathResource("redis/synchronize-reservation.lua"));
        SYNC.setResultType(Long.class);
    }
    private static final DefaultRedisScript<Long> ADD_ACTIVITY = new DefaultRedisScript<>("""
            if redis.call('HEXISTS', KEYS[1], 'a:' .. ARGV[1]) == 1 then return 0 end
            redis.call('HSET', KEYS[1], 'a:' .. ARGV[1], ARGV[2], 'stock:' .. ARGV[1], ARGV[3])
            return 1
            """, Long.class);
    private static final DefaultRedisScript<Long> INITIALIZE = new DefaultRedisScript<>("""
            if redis.call('HGET', KEYS[1], 'ready') == '1' then return 0 end
            for field, value in pairs(cjson.decode(ARGV[1])) do redis.call('HSET', KEYS[1], field, value) end
            for _, id in ipairs(cjson.decode(ARGV[2])) do redis.call('ZADD', KEYS[2], 0, id) end
            redis.call('HSET', KEYS[1], 'ready', '1')
            return 1
            """, Long.class);
    private static final DefaultRedisScript<List> CLAIM = new DefaultRedisScript<>("""
            local clock = redis.call('TIME')
            local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
            local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 64)
            local result = {}
            for _, id in ipairs(ids) do
                local request = redis.call('HGET', KEYS[1], 'r:' .. id)
                if request then
                    redis.call('ZADD', KEYS[2], now + 5000, id)
                    table.insert(result, request)
                end
            end
            return result
            """, List.class);
    private static final DefaultRedisScript<List> HISTORY = new DefaultRedisScript<>("""
            local result = {}
            local ids = cjson.decode(redis.call('HGET', KEYS[1], 'history:' .. ARGV[1]) or '[]')
            for _, id in ipairs(ids) do
                local request = redis.call('HGET', KEYS[1], 'r:' .. id)
                if request then table.insert(result, request) end
            end
            return result
            """, List.class);

    private final SaleStore store;
    private final StringRedisTemplate redis;
    private final ObjectMapper json;
    private final TransactionTemplate transactions;

    public Preallocation(SaleStore store, StringRedisTemplate redis, ObjectMapper json, PlatformTransactionManager manager) {
        this.store = store;
        this.redis = redis;
        this.json = json;
        this.transactions = new TransactionTemplate(manager);
        this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public ReservationView submit(String tenant, Reserve input) {
        try {
            String result = reserve(tenant, input);
            if ("INITIALIZE".equals(result)) {
                initialize(tenant);
                result = reserve(tenant, input);
            }
            if ("ACTIVITY_MISSING".equals(result)) {
                loadActivity(tenant, input.activityId().toString());
                result = reserve(tenant, input);
            }
            if (result != null && result.startsWith("{")) { return json.readValue(result, Entry.class).view(); }
            throw ApiException.conflict(result == null ? "PREALLOCATION_UNAVAILABLE" : result, "请检查活动状态、数量及原请求编号");
        } catch (DataAccessException error) {
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "PREALLOCATION_UNAVAILABLE", "库存预扣暂时不可用，请使用原请求编号重试");
        }
    }

    private String reserve(String tenant, Reserve input) {
        return redis.execute(RESERVE, keys(tenant), input.requestId().toString(), input.activityId().toString(),
                input.userId(), Integer.toString(input.quantity()));
    }

    public Optional<ReservationView> find(String tenant, String id) {
        String value = (String) redis.opsForHash().get(keys(tenant).getFirst(), "r:" + id);
        return Optional.ofNullable(value).map(item -> json.readValue(item, Entry.class).view());
    }

    public List<ReservationView> history(String tenant, String user) {
        List<?> entries = redis.execute(HISTORY, List.of(keys(tenant).getFirst()), user);
        return entries.stream().map(item -> json.readValue((String) item, Entry.class).view()).toList();
    }

    public List<RequestMessage> claim(String tenant) {
        List<?> entries = redis.execute(CLAIM, keys(tenant));
        return entries.stream().map(item -> json.readValue((String) item, Entry.class).view())
                .map(view -> new RequestMessage(tenant, view.requestId(), view)).toList();
    }

    public long pendingCount(String tenant) {
        Long count = redis.opsForZSet().zCard(keys(tenant).get(1));
        return count == null ? 0 : count;
    }

    public void synchronize(String tenant, ReservationView result) {
        Long updated = redis.execute(SYNC, keys(tenant), json.writeValueAsString(Entry.from(result)));
        if (!Long.valueOf(1).equals(updated)) {
            initialize(tenant);
            updated = redis.execute(SYNC, keys(tenant), json.writeValueAsString(Entry.from(result)));
            if (!Long.valueOf(1).equals(updated)) { throw new IllegalStateException("Redis admission state unavailable"); }
        }
    }

    public void synchronizeQuietly(String tenant, ReservationView result) {
        try { synchronize(tenant, result); }
        catch (DataAccessException error) { log.warn("Redis result synchronization deferred for {}", result.requestId()); }
    }

    private void initialize(String tenant) {
        transactions.executeWithoutResult(status -> {
            var fields = new HashMap<String, String>();
            var pending = new ArrayList<String>();
            // 锁定活动后读取额度与申请，确保首次导入的数据库库存和已用额度来自同一业务边界。
            var activities = store.jdbc().sql("SELECT * FROM activities WHERE tenant_id=? ORDER BY id FOR UPDATE")
                    .param(tenant).query(SaleStore::activityRow).list();
            for (var activity : activities) { addActivity(fields, activity); }
            store.jdbc().sql("SELECT activity_id,user_id,used_quantity FROM user_quotas WHERE tenant_id=?")
                    .param(tenant).query((row, index) -> {
                        fields.put("q:" + row.getString("activity_id") + ":" + row.getString("user_id"), row.getString("used_quantity"));
                        return row.getString("user_id");
                    }).list();
            var requests = store.jdbc().sql("SELECT * FROM reservations WHERE tenant_id=?")
                    .param(tenant).query(SaleStore::reservationRow).list();
            for (var request : requests) {
                fields.put("r:" + request.requestId(), json.writeValueAsString(Entry.from(request)));
                if (request.state().equals("QUEUED")) { pending.add(request.requestId()); }
            }
            // shortcut: 初始化加载租户全部申请，数据量增长时需增加受控归档与分批恢复。
            redis.execute(INITIALIZE, keys(tenant), json.writeValueAsString(fields), json.writeValueAsString(pending));
        });
    }

    private void loadActivity(String tenant, String id) {
        transactions.executeWithoutResult(status -> {
            var activity = store.activity(tenant, id, true);
            var fields = new HashMap<String, String>();
            addActivity(fields, activity);
            redis.execute(ADD_ACTIVITY, List.of(keys(tenant).getFirst()), id, fields.get("a:" + id), fields.get("stock:" + id));
        });
    }

    private void addActivity(Map<String, String> fields, ActivityView activity) {
        fields.put("a:" + activity.id(), json.writeValueAsString(new Rules(activity.limitPerUser(),
                activity.queueTimeoutSeconds(), activity.startsAt().toEpochMilli(), activity.endsAt().toEpochMilli())));
        fields.put("stock:" + activity.id(), Integer.toString(activity.availableStock()));
    }

    private List<String> keys(String tenant) {
        String prefix = "flashsale:preallocation:{" + tenant + "}:";
        return List.of(prefix + "state", prefix + "pending");
    }

    record Rules(int limitPerUser, int queueTimeoutSeconds, long startsAtMs, long endsAtMs) {}

    record Entry(String requestId, String activityId, String userId, int quantity, String state, String reason,
                 String orderId, long queueDeadlineMs, Long expiresAtMs, long createdAtMs, long updatedAtMs,
                 boolean funded, boolean persisted) {
        static Entry from(ReservationView view) {
            return new Entry(view.requestId(), view.activityId(), view.userId(), view.quantity(), view.state(), view.reason(),
                    view.orderId(), view.queueDeadline().toEpochMilli(), view.expiresAt() == null ? null : view.expiresAt().toEpochMilli(),
                    view.createdAt().toEpochMilli(), view.updatedAt().toEpochMilli(),
                    view.state().equals("RESERVED") || view.state().equals("CONFIRMED"), !view.state().equals("QUEUED"));
        }

        ReservationView view() {
            return new ReservationView(requestId, activityId, userId, quantity, state, reason, orderId,
                    Instant.ofEpochMilli(queueDeadlineMs), expiresAtMs == null ? null : Instant.ofEpochMilli(expiresAtMs),
                    Instant.ofEpochMilli(createdAtMs), Instant.ofEpochMilli(updatedAtMs));
        }
    }
}
