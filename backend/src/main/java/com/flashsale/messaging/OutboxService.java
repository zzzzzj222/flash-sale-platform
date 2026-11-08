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

    public OutboxService(SaleStore store) { this.store = store; }

    public record Delivery(String id, String tenant, String requestId, String destination, String payload,
                           int attempts, String lease) {}

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public List<Delivery> claim() {
        long now = store.now();
        String lease = UUID.randomUUID().toString();
        var events = store.jdbc().sql("""
                SELECT * FROM outbox WHERE available_at_ms<=?
                AND (status='READY' OR (status='IN_FLIGHT' AND locked_until_ms<=?))
                ORDER BY available_at_ms,id LIMIT 16 FOR UPDATE SKIP LOCKED
                """).params(now, now).query((row, index) -> new Delivery(row.getString("id"),
                row.getString("tenant_id"), row.getString("request_id"), row.getString("destination"),
                row.getString("payload"), row.getInt("attempts") + 1, lease)).list();
        for (var event : events) {
            store.jdbc().sql("UPDATE outbox SET status='IN_FLIGHT',attempts=attempts+1,lease_owner=?,locked_until_ms=? WHERE id=?")
                    .params(lease, now + 60000, event.id()).update();
        }
        return events;
    }

    public void delivered(Delivery event) {
        store.jdbc().sql("UPDATE outbox SET status='DELIVERED',last_error=NULL WHERE id=? AND lease_owner=? AND status='IN_FLIGHT'")
                .params(event.id(), event.lease()).update();
    }

    public void failed(Delivery event, String error) {
        long delay = Math.min(60000L, 500L * (1L << Math.min(event.attempts(), 7)));
        String status = event.destination().equals("WEBHOOK") && event.attempts() >= 12 ? "DEAD" : "READY";
        store.jdbc().sql("UPDATE outbox SET status=?,available_at_ms=?,last_error=? WHERE id=? AND lease_owner=? AND status='IN_FLIGHT'")
                .params(status, store.now() + delay, error, event.id(), event.lease()).update();
    }

    public List<EventView> failedEvents(String tenant) {
        return store.jdbc().sql("SELECT * FROM outbox WHERE tenant_id=? AND status='DEAD' ORDER BY created_at_ms DESC LIMIT 100")
                .param(tenant).query((row, index) -> new EventView(row.getString("id"), row.getString("request_id"),
                        row.getString("destination"), row.getString("status"), row.getInt("attempts"), row.getString("last_error"))).list();
    }

    public void retry(String tenant, String id) {
        int changed = store.jdbc().sql("""
                UPDATE outbox SET status='READY',attempts=0,available_at_ms=?,last_error=NULL
                WHERE id=? AND tenant_id=? AND destination='WEBHOOK' AND status='DEAD'
                """).params(store.now(), id, tenant).update();
        if (changed == 0) { throw ApiException.notFound(); }
    }
}
