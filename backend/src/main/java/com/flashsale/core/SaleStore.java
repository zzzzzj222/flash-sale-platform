package com.flashsale.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import com.flashsale.api.ApiException;
import com.flashsale.api.Contracts.ActivityView;
import com.flashsale.api.Contracts.ReservationView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Repository
public class SaleStore {
    private static final Logger log = LoggerFactory.getLogger(SaleStore.class);
    private final JdbcClient jdbc;
    private final StringRedisTemplate redis;
    private final ObjectMapper json;

    /**
     * 初始化业务数据访问及活动规则缓存组件。
     */
    public SaleStore(JdbcClient jdbc, StringRedisTemplate redis, ObjectMapper json) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.json = json;
    }

    /**
     * 返回底层 JDBC 客户端，供业务组件执行数据库操作。
     */
    public JdbcClient jdbc() { return jdbc; }

    /**
     * 返回数据库当前时间的 Unix 毫秒时间戳，统一业务超时判断的时间来源。
     */
    public long now() {
        return jdbc.sql("SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS UNSIGNED)")
                .query(Long.class).single();
    }

    /**
     * 按租户和活动编号查询活动，不存在时抛出资源未找到异常。
     *
     * @param lock 是否追加 FOR UPDATE，由调用方在事务内持有行锁
     */
    public ActivityView activity(String tenant, String id, boolean lock) {
        // 事务内直读数据库，避免缓存未提交数据或使用缓存参与库存判断。
        if (lock || TransactionSynchronizationManager.isActualTransactionActive()) {
            return jdbc.sql("SELECT * FROM activities WHERE tenant_id=? AND id=?" + (lock ? " FOR UPDATE" : ""))
                    .params(tenant, id).query(SaleStore::activityRow).optional().orElseThrow(ApiException::notFound);
        }
        int stock = jdbc.sql("SELECT available_stock FROM activities WHERE tenant_id=? AND id=?")
                .params(tenant, id).query(Integer.class).optional().orElseThrow(ApiException::notFound);
        return activityMetadata(tenant, List.of(id)).get(id).view(stock);
    }

    /**
     * 返回最近 100 个活动，批量缓存活动规则并实时查询可用库存。
     */
    public List<ActivityView> activities(String tenant) {
        // shortcut: fixed list limits suit the demo; add cursor pagination before large catalogs.
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            return jdbc.sql("SELECT * FROM activities WHERE tenant_id=? ORDER BY created_at_ms DESC LIMIT 100")
                    .param(tenant).query(SaleStore::activityRow).list();
        }
        var stocks = jdbc.sql("SELECT id,available_stock FROM activities WHERE tenant_id=? ORDER BY created_at_ms DESC LIMIT 100")
                .param(tenant).query((row, index) -> new ActivityStock(row.getString("id"), row.getInt("available_stock"))).list();
        if (stocks.isEmpty()) { return List.of(); }
        var metadata = activityMetadata(tenant, stocks.stream().map(ActivityStock::id).toList());
        return stocks.stream().map(stock -> metadata.get(stock.id()).view(stock.availableStock())).toList();
    }

    // shortcut: 活动规则创建后不可修改，新增编辑入口时须同步失效规则缓存。
    private Map<String, ActivityMetadata> activityMetadata(String tenant, List<String> ids) {
        var result = new HashMap<String, ActivityMetadata>();
        var keys = ids.stream().map(id -> "flashsale:activity:metadata:v1:" + tenant + ":" + id).toList();
        boolean cacheAvailable = true;
        try {
            var cached = redis.opsForValue().multiGet(keys);
            if (cached != null) {
                for (int index = 0; index < ids.size(); index++) {
                    if (cached.get(index) != null) {
                        var item = json.readValue(cached.get(index), ActivityMetadata.class);
                        if (item != null && ids.get(index).equals(item.id())) { result.put(item.id(), item); }
                    }
                }
            }
        } catch (DataAccessException error) {
            cacheAvailable = false;
            log.debug("Activity metadata cache read failed", error);
        } catch (JacksonException error) {
            log.debug("Activity metadata cache read failed", error);
        }
        var missing = ids.stream().filter(id -> !result.containsKey(id)).toList();
        if (!missing.isEmpty()) {
            var loaded = jdbc.sql("SELECT * FROM activities WHERE tenant_id=:tenant AND id IN (:ids)")
                    .param("tenant", tenant).param("ids", missing).query(SaleStore::activityRow).list()
                    .stream().map(ActivityMetadata::from).toList();
            for (var item : loaded) { result.put(item.id(), item); }
            if (cacheAvailable) {
                try {
                    for (var item : loaded) {
                        redis.opsForValue().set("flashsale:activity:metadata:v1:" + tenant + ":" + item.id(),
                                json.writeValueAsString(item), Duration.ofSeconds(ThreadLocalRandom.current().nextLong(300, 361)));
                    }
                } catch (DataAccessException | JacksonException error) {
                    log.debug("Activity metadata cache write failed", error);
                }
            }
        }
        if (result.size() != ids.size()) { throw ApiException.notFound(); }
        return result;
    }

    private record ActivityStock(String id, int availableStock) {}

    record ActivityMetadata(String id, String resourceId, String title, int stock, int limitPerUser,
                            int holdSeconds, int queueTimeoutSeconds, Instant startsAt, Instant endsAt) {
        static ActivityMetadata from(ActivityView activity) {
            return new ActivityMetadata(activity.id(), activity.resourceId(), activity.title(), activity.stock(),
                    activity.limitPerUser(), activity.holdSeconds(), activity.queueTimeoutSeconds(),
                    activity.startsAt(), activity.endsAt());
        }

        ActivityView view(int availableStock) {
            return new ActivityView(id, resourceId, title, stock, availableStock, limitPerUser,
                    holdSeconds, queueTimeoutSeconds, startsAt, endsAt);
        }
    }

    /**
     * 按租户和请求编号查询预占记录，不存在时返回空 Optional。
     *
     * @param lock 是否追加 FOR UPDATE，由调用方在事务内持有行锁
     */
    public Optional<ReservationView> find(String tenant, String requestId, boolean lock) {
        return jdbc.sql("SELECT * FROM reservations WHERE tenant_id=? AND request_id=?" + (lock ? " FOR UPDATE" : ""))
                .params(tenant, requestId).query(SaleStore::reservationRow).optional();
    }

    /**
     * 按租户和请求编号获取预占记录，不存在时抛出资源未找到异常。
     *
     * @param lock 是否追加 FOR UPDATE，由调用方在事务内持有行锁
     */
    public ReservationView reservation(String tenant, String requestId, boolean lock) {
        return find(tenant, requestId, lock).orElseThrow(ApiException::notFound);
    }

    /**
     * 将活动结果集的当前行映射为活动视图，并将毫秒时间戳转换为 Instant。
     *
     * @param index JDBC 行映射器传入的行号，本方法无需使用
     * @throws SQLException 读取结果集失败时抛出
     */
    public static ActivityView activityRow(ResultSet row, int index) throws SQLException {
        return new ActivityView(row.getString("id"), row.getString("resource_id"), row.getString("title"),
                row.getInt("total_stock"), row.getInt("available_stock"), row.getInt("limit_per_user"),
                row.getInt("hold_seconds"), row.getInt("queue_timeout_seconds"),
                Instant.ofEpochMilli(row.getLong("starts_at_ms")), Instant.ofEpochMilli(row.getLong("ends_at_ms")));
    }

    /**
     * 将预占结果集的当前行映射为预占视图，保留可空的预占到期时间。
     *
     * @param index JDBC 行映射器传入的行号，本方法无需使用
     * @throws SQLException 读取结果集失败时抛出
     */
    public static ReservationView reservationRow(ResultSet row, int index) throws SQLException {
        long expires = row.getLong("expires_at_ms");
        Instant expiresAt = row.wasNull() ? null : Instant.ofEpochMilli(expires);
        return new ReservationView(row.getString("request_id"), row.getString("activity_id"), row.getString("user_id"),
                row.getInt("quantity"), row.getString("state"), row.getString("reason"), row.getString("order_id"),
                Instant.ofEpochMilli(row.getLong("queue_deadline_ms")), expiresAt,
                Instant.ofEpochMilli(row.getLong("created_at_ms")), Instant.ofEpochMilli(row.getLong("updated_at_ms")));
    }
}
