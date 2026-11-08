package com.flashsale.messaging;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import jakarta.annotation.PreDestroy;
import com.flashsale.SaleProperties;
import com.flashsale.core.SaleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "sale.workers-enabled", havingValue = "true", matchIfMissing = true)
public class OutboxDispatcher {
    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);
    private final OutboxService outbox;
    private final SaleService sales;
    private final SaleProperties properties;
    private final RabbitTemplate rabbit;
    private final ExecutorService deliveries = Executors.newFixedThreadPool(16,
            Thread.ofVirtual().name("outbox-", 0).factory());
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NEVER).build();

    public OutboxDispatcher(OutboxService outbox, SaleService sales, SaleProperties properties, RabbitTemplate rabbit) {
        this.outbox = outbox;
        this.sales = sales;
        this.properties = properties;
        this.rabbit = rabbit;
    }

    @Scheduled(fixedDelayString = "${sale.dispatch-delay-ms:100}")
    public void dispatch() {
        var tasks = outbox.claim().stream().map(event -> CompletableFuture.runAsync(() -> deliver(event), deliveries)).toList();
        CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new)).join();
    }

    private void deliver(OutboxService.Delivery event) {
        try {
            if (event.destination().equals("RABBIT")) { publish(event); }
            else { webhook(event); }
            outbox.delivered(event);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            outbox.failed(event, "INTERRUPTED");
        } catch (Exception error) {
            log.warn("Event {} attempt {} failed: {}", event.id(), event.attempts(), error.getClass().getSimpleName());
            outbox.failed(event, error.getClass().getSimpleName());
        }
    }

    @PreDestroy
    void stop() {
        deliveries.shutdown();
        try {
            if (!deliveries.awaitTermination(5, TimeUnit.SECONDS)) { deliveries.shutdownNow(); }
        } catch (InterruptedException error) {
            deliveries.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void publish(OutboxService.Delivery event) throws Exception {
        if (!sales.result(event.tenant(), event.requestId()).state().equals("QUEUED")) { return; }
        var messageProperties = new MessageProperties();
        messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        messageProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        messageProperties.setMessageId(event.id());
        var correlation = new CorrelationData(event.id());
        rabbit.send("", properties.queueName(), new Message(event.payload().getBytes(StandardCharsets.UTF_8), messageProperties), correlation);
        var confirmation = correlation.getFuture().get(3, TimeUnit.SECONDS);
        if (!confirmation.ack() || correlation.getReturned() != null) {
            throw new IllegalStateException("Broker did not accept event");
        }
    }

    private void webhook(OutboxService.Delivery event) throws Exception {
        var client = properties.client(event.tenant());
        if (!client.hasWebhook()) { throw new IllegalStateException("Webhook configuration removed"); }
        String timestamp = Long.toString(Instant.now().getEpochSecond());
        String signature = sign(client.webhookSecret(), event.id() + "." + timestamp + "." + event.payload());
        var request = HttpRequest.newBuilder(URI.create(client.webhookUrl())).timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/json; charset=utf-8")
                .header("X-Event-Id", event.id()).header("X-Event-Timestamp", timestamp)
                .header("X-Signature", "sha256=" + signature)
                .POST(HttpRequest.BodyPublishers.ofString(event.payload(), StandardCharsets.UTF_8)).build();
        int status = http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        if (status < 200 || status >= 300) { throw new WebhookRejectedException(status); }
    }

    public static String sign(String secret, String value) {
        try {
            var mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.GeneralSecurityException error) {
            throw new IllegalStateException(error);
        }
    }

    private static class WebhookRejectedException extends RuntimeException {
        WebhookRejectedException(int status) { super("HTTP " + status); }
    }
}
