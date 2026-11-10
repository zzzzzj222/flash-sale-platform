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
import com.flashsale.core.Preallocation;
import com.flashsale.api.Contracts.RequestMessage;
import com.flashsale.api.Contracts.ReservationView;
import org.springframework.dao.DataAccessException;
import java.util.ArrayList;
import tools.jackson.databind.ObjectMapper;
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
    private final Preallocation preallocation;
    private final ObjectMapper json;
    private final ExecutorService deliveries = Executors.newFixedThreadPool(16,
            Thread.ofVirtual().name("outbox-", 0).factory());
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2))
            .followRedirects(HttpClient.Redirect.NEVER).build();

    /**
     * 初始化事件调度器使用的业务服务、消息发布、Redis 预分配及 JSON 序列化依赖。
     *
     * @param outbox 事件领取和投递结果记录服务
     * @param sales 秒杀申请处理与结果查询服务
     * @param properties 租户、队列和 Webhook 配置
     * @param rabbit RabbitMQ 消息发布客户端
     * @param preallocation Redis 预分配与状态同步组件
     * @param json JSON 序列化与反序列化组件
     */
    public OutboxDispatcher(OutboxService outbox, SaleService sales, SaleProperties properties, RabbitTemplate rabbit,
                            Preallocation preallocation, ObjectMapper json) {
        this.outbox = outbox;
        this.sales = sales;
        this.properties = properties;
        this.rabbit = rabbit;
        this.preallocation = preallocation;
        this.json = json;
    }

    /**
     * 定时领取各租户的 Redis 待处理申请及数据库 outbox 事件，并发投递后等待本批任务完成。
     * 单个租户的 Redis 数据访问失败时记录日志，并继续处理其他租户和 outbox 事件。
     */
    @Scheduled(fixedDelayString = "${sale.dispatch-delay-ms:100}")
    public void dispatch() {
        var tasks = new ArrayList<CompletableFuture<Void>>();
        for (var client : properties.clients()) {
            try {
                for (var request : preallocation.claim(client.id())) {
                    tasks.add(CompletableFuture.runAsync(() -> deliverRequest(request), deliveries));
                }
            } catch (DataAccessException error) {
                log.warn("Redis pending requests unavailable for {}", client.id());
            }
        }
        for (var event : outbox.claim()) {
            tasks.add(CompletableFuture.runAsync(() -> deliver(event), deliveries));
        }
        CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new)).join();
    }

    /**
     * 将未超过排队截止时间的申请发布到 RabbitMQ，已到期的申请直接交由业务服务处理。
     * 投递异常时保留后续重试机会，中断时恢复线程的中断标记。
     *
     * @param request 从 Redis 领取的待处理申请
     */
    private void deliverRequest(RequestMessage request) {
        try {
            if (request.reservation().queueDeadline().toEpochMilli() <= System.currentTimeMillis()) {
                sales.process(request);
            } else {
                publish(request.requestId(), json.writeValueAsString(request));
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        } catch (Exception error) {
            log.warn("Request {} delivery deferred: {}", request.requestId(), error.getClass().getSimpleName());
        }
    }

    /**
     * 按目标将事件发送到 RabbitMQ、同步到 Redis 或调用 Webhook，并记录投递结果。
     * 失败时登记重试或终止状态，中断时同时恢复线程的中断标记。
     *
     * @param event 已领取并携带租约的事件
     */
    private void deliver(OutboxService.Delivery event) {
        try {
            if (event.destination().equals("RABBIT")) { publish(event); }
            else if (event.destination().equals("REDIS")) {
                preallocation.synchronize(event.tenant(), json.readValue(event.payload(), ReservationView.class));
            }
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

    /**
     * 应用销毁时关闭投递线程池，最多等待 5 秒，超时或被中断时强制停止剩余任务。
     * 等待被中断时恢复当前线程的中断标记。
     */
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

    /**
     * 仅当关联申请仍处于 QUEUED 状态时发布事件，其他状态直接跳过。
     *
     * @param event 目标为 RabbitMQ 的 outbox 事件
     * @throws Exception 申请查询、消息发送或发布确认失败时抛出
     */
    private void publish(OutboxService.Delivery event) throws Exception {
        if (!sales.result(event.tenant(), event.requestId()).state().equals("QUEUED")) { return; }
        publish(event.id(), event.payload());
    }

    /**
     * 将 JSON 作为持久化消息发送到请求队列，并最多等待 3 秒获取发布确认。
     * Broker 未确认接收或消息被退回时抛出异常，交由调用方处理失败。
     *
     * @param messageId 消息编号，同时用作发布确认的关联编号
     * @param payload 按 UTF-8 编码发送的 JSON 内容
     * @throws Exception 消息发送失败、等待确认超时或中断、Broker 拒收或退回消息时抛出
     */
    private void publish(String messageId, String payload) throws Exception {
        var messageProperties = new MessageProperties();
        messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        messageProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        messageProperties.setMessageId(messageId);
        var correlation = new CorrelationData(messageId);
        rabbit.send("", properties.queueName(), new Message(payload.getBytes(StandardCharsets.UTF_8), messageProperties), correlation);
        var confirmation = correlation.getFuture().get(3, TimeUnit.SECONDS);
        if (!confirmation.ack() || correlation.getReturned() != null) {
            throw new IllegalStateException("Broker did not accept event");
        }
    }

    /**
     * 向租户配置的 Webhook 地址发送带事件编号、秒级时间戳和 HMAC 签名的 JSON 通知。
     * 请求超时为 3 秒，仅将 HTTP 2xx 响应视为成功。
     *
     * @param event 待通知的事件，签名内容依次为事件编号、时间戳和原始载荷，以点号分隔
     * @throws Exception Webhook 配置缺失、签名或 HTTP 调用失败、响应非 2xx 时抛出
     */
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

    /**
     * 使用 UTF-8 编码的密钥和内容计算 HmacSHA256 签名。
     *
     * @param secret 签名密钥
     * @param value 待签名的原始字符串
     * @return 小写十六进制签名，不包含 sha256= 前缀
     * @throws IllegalStateException 签名算法不可用或密钥初始化失败时抛出
     */
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
        /**
         * 将 Webhook 返回的非 2xx 状态码保存为异常消息。
         *
         * @param status Webhook 响应的 HTTP 状态码
         */
        WebhookRejectedException(int status) { super("HTTP " + status); }
    }
}
