package com.flashsale.messaging;

import java.util.List;
import java.util.UUID;
import com.flashsale.api.ApiException;
import com.flashsale.api.Contracts.EventView;
import com.flashsale.core.SaleStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Isolation;

@Service
public class OutboxService {
    private final SaleStore store;

    /**
     * 初始化事件存储访问，复用数据库客户端和时间来源。
     *
     * @param store 秒杀数据存储
     */
    public OutboxService(SaleStore store) { this.store = store; }

    /**
     * 保存一次投递所需的数据、当前尝试次数及用于校验投递归属的租约标识。
     *
     * @param id 事件编号
     * @param tenant 所属租户编号
     * @param requestId 关联的申请编号
     * @param destination 投递目标类型
     * @param payload 待投递的 JSON 内容
     * @param attempts 包含本次投递的尝试次数
     * @param lease 本批领取生成的租约标识
     */
    public record Delivery(String id, String tenant, String requestId, String destination, String payload,
                           int attempts, String lease) {}

    /**
     * 在事务中领取最多 64 条到期可投递事件，包括租约已过期的处理中事件。
     * 跳过其他事务已锁定的行，为本批事件设置 60 秒租约并递增尝试次数。
     *
     * @return 携带本次租约和更新后尝试次数的事件列表
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public List<Delivery> claim() {
        return claim(List.of("RABBIT", "REDIS", "WEBHOOK"), 64);
    }

    /** 按投递目标隔离领取任务，同一批事件使用一个租约并一次更新领取状态。 */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public List<Delivery> claim(List<String> destinations, int limit) {
        if (destinations.isEmpty() || limit < 1 || limit > 64) { throw new IllegalArgumentException("Invalid outbox batch"); }
        long now = store.now();
        String lease = UUID.randomUUID().toString();
        var events = store.jdbc().sql("""
                SELECT * FROM outbox WHERE destination IN (:destinations) AND available_at_ms<=:now
                AND (status='READY' OR (status='IN_FLIGHT' AND locked_until_ms<=:now))
                ORDER BY available_at_ms,id LIMIT :limit FOR UPDATE SKIP LOCKED
                """).param("destinations", destinations).param("now", now).param("limit", limit)
                .query((row, index) -> new Delivery(row.getString("id"),
                row.getString("tenant_id"), row.getString("request_id"), row.getString("destination"),
                row.getString("payload"), row.getInt("attempts") + 1, lease)).list();
        if (!events.isEmpty()) {
            store.jdbc().sql("""
                    UPDATE outbox SET status='IN_FLIGHT',attempts=attempts+1,lease_owner=:lease,locked_until_ms=:until
                    WHERE id IN (:ids)
                    """).param("lease", lease).param("until", now + 60000)
                    .param("ids", events.stream().map(Delivery::id).toList()).update();
        }
        return events;
    }

    /**
     * 将仍由本次租约持有的处理中事件标记为投递成功，并清除错误记录。
     *
     * @param event 本次已成功投递的事件
     */
    public void delivered(Delivery event) {
        delivered(List.of(event));
    }

    /** 只完成仍属于同一领取租约的事件，过期回调不能覆盖新投递者的结果。 */
    public void delivered(List<Delivery> events) {
        if (events.isEmpty()) { return; }
        String lease = events.getFirst().lease();
        if (events.stream().anyMatch(event -> !lease.equals(event.lease()))) {
            throw new IllegalArgumentException("Outbox batch must share one lease");
        }
        store.jdbc().sql("""
                UPDATE outbox SET status='DELIVERED',last_error=NULL
                WHERE id IN (:ids) AND lease_owner=:lease AND status='IN_FLIGHT'
                """).param("ids", events.stream().map(Delivery::id).toList()).param("lease", lease).update();
    }

    /**
     * 对仍由本次租约持有的处理中事件记录失败，并按指数退避安排重试，最长延迟 60 秒。
     * Webhook 尝试达到 12 次后标记为 DEAD，其他失败事件恢复为 READY。
     *
     * @param event 本次投递失败的事件
     * @param error 记录到事件中的错误摘要
     */
    public void failed(Delivery event, String error) {
        long delay = Math.min(60000L, 500L * (1L << Math.min(event.attempts(), 7)));
        String status = event.destination().equals("WEBHOOK") && event.attempts() >= 12 ? "DEAD" : "READY";
        store.jdbc().sql("UPDATE outbox SET status=?,available_at_ms=?,last_error=? WHERE id=? AND lease_owner=? AND status='IN_FLIGHT'")
                .params(status, store.now() + delay, error, event.id(), event.lease()).update();
    }

    /**
     * 按创建时间倒序查询指定租户最近的 DEAD 事件，最多返回 100 条。
     *
     * @param tenant 所属租户编号
     * @return 已停止自动重试的事件列表
     */
    public List<EventView> failedEvents(String tenant) {
        return store.jdbc().sql("SELECT * FROM outbox WHERE tenant_id=? AND status='DEAD' ORDER BY created_at_ms DESC LIMIT 100")
                .param(tenant).query((row, index) -> new EventView(row.getString("id"), row.getString("request_id"),
                        row.getString("destination"), row.getString("status"), row.getInt("attempts"), row.getString("last_error"))).list();
    }

    /**
     * 将指定租户的 DEAD Webhook 事件恢复为立即可投递状态，重置尝试次数并清除错误。
     *
     * @param tenant 所属租户编号
     * @param id 需要重新投递的事件编号
     * @throws ApiException 找不到属于该租户且符合重试条件的事件时抛出 404 异常
     */
    public void retry(String tenant, String id) {
        int changed = store.jdbc().sql("""
                UPDATE outbox SET status='READY',attempts=0,available_at_ms=?,last_error=NULL
                WHERE id=? AND tenant_id=? AND destination='WEBHOOK' AND status='DEAD'
                """).params(store.now(), id, tenant).update();
        if (changed == 0) { throw ApiException.notFound(); }
    }
}
