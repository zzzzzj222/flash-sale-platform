package com.flashsale.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import com.flashsale.api.ApiException;
import com.flashsale.api.Contracts.ActivityView;
import com.flashsale.api.Contracts.ReservationView;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class SaleStore {
    private final JdbcClient jdbc;

    /**
     * 初始化活动、预占及相关业务数据共用的 JDBC 客户端。
     */
    public SaleStore(JdbcClient jdbc) { this.jdbc = jdbc; }

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
        return jdbc.sql("SELECT * FROM activities WHERE tenant_id=? AND id=?" + (lock ? " FOR UPDATE" : ""))
                .params(tenant, id).query(SaleStore::activityRow).optional().orElseThrow(ApiException::notFound);
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
