package com.flashsale.core;

import java.util.concurrent.atomic.AtomicLong;
import com.flashsale.SaleProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.dao.DataAccessException;

@Component
public class OperationalMetrics {
    private final SaleStore store;
    private final Preallocation preallocation;
    private final SaleProperties properties;
    private final AtomicLong queued = new AtomicLong();
    private final AtomicLong reserved = new AtomicLong();
    private final AtomicLong failedEvents = new AtomicLong();
    private final AtomicLong queueAge = new AtomicLong();
    private final AtomicLong pendingPersistence = new AtomicLong();

    /**
     * 初始化数据访问组件，并注册排队数、预占数、死信事件数和最早排队请求等待时长指标。
     */
    public OperationalMetrics(SaleStore store, MeterRegistry registry, Preallocation preallocation, SaleProperties properties) {
        this.store = store;
        this.preallocation = preallocation;
        this.properties = properties;
        Gauge.builder("flashsale.requests.queued", queued, AtomicLong::doubleValue).register(registry);
        Gauge.builder("flashsale.requests.reserved", reserved, AtomicLong::doubleValue).register(registry);
        Gauge.builder("flashsale.events.failed", failedEvents, AtomicLong::doubleValue).register(registry);
        Gauge.builder("flashsale.queue.oldest.age.seconds", queueAge, AtomicLong::doubleValue).register(registry);
        Gauge.builder("flashsale.requests.pending.persistence", pendingPersistence, AtomicLong::doubleValue).register(registry);
    }

    /**
     * 定期从数据库刷新各项指标，等待时长以秒计，无排队请求时记为零。
     */
    @Scheduled(fixedDelay = 5000)
    public void update() {
        queued.set(store.jdbc().sql("SELECT COUNT(*) FROM reservations WHERE state='QUEUED'").query(Long.class).single());
        reserved.set(store.jdbc().sql("SELECT COUNT(*) FROM reservations WHERE state='RESERVED'").query(Long.class).single());
        failedEvents.set(store.jdbc().sql("SELECT COUNT(*) FROM outbox WHERE status='DEAD'").query(Long.class).single());
        long oldest = store.jdbc().sql("SELECT COALESCE(MIN(created_at_ms),0) FROM reservations WHERE state='QUEUED'").query(Long.class).single();
        queueAge.set(oldest == 0 ? 0 : Math.max(0, (store.now() - oldest) / 1000));
        try {
            pendingPersistence.set(properties.clients().stream().mapToLong(client -> preallocation.pendingCount(client.id())).sum());
        } catch (DataAccessException error) { pendingPersistence.set(-1); }
    }
}
