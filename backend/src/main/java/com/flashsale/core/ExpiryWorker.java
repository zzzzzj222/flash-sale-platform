package com.flashsale.core;

import com.flashsale.api.Contracts.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "sale.workers-enabled", havingValue = "true", matchIfMissing = true)
public class ExpiryWorker {
    private static final Logger log = LoggerFactory.getLogger(ExpiryWorker.class);
    private final SaleStore store;
    private final SaleService service;

    /**
     * 初始化过期扫描使用的数据访问组件和预占业务服务。
     */
    public ExpiryWorker(SaleStore store, SaleService service) { this.store = store; this.service = service; }

    /**
     * 按数据库时间扫描最多 100 条已超时的预占或排队请求，并逐条执行过期处理。
     * 单条处理失败时记录警告，继续处理本批次的其他请求。
     */
    @Scheduled(fixedDelayString = "${sale.expiry-delay-ms:500}")
    public void expire() {
        long now = store.now();
        var requests = store.jdbc().sql("""
                SELECT tenant_id,request_id FROM reservations
                WHERE (state='RESERVED' AND expires_at_ms<=?) OR (state='QUEUED' AND queue_deadline_ms<=?)
                ORDER BY updated_at_ms LIMIT 100
                """).params(now, now).query((row, index) -> new RequestMessage(row.getString(1), row.getString(2))).list();
        for (var request : requests) {
            try { service.expire(request.tenantId(), request.requestId()); }
            catch (RuntimeException error) {
                log.warn("Expiry {} failed: {}", request.requestId(), error.getClass().getSimpleName());
            }
        }
    }
}
