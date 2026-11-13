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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.List;
import java.util.LinkedHashMap;
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
    private final ExecutorService publications = executor("publish-", 16, 128);
    private final ExecutorService expirations = executor("pending-expiry-", 4, 32);
    private final ExecutorService synchronizations = executor("redis-sync-", 4, 64);
    private final ExecutorService deliveries = executor("notification-", 4, 16);
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
     * 领取 Redis 待处理申请，同活动按 sale.batch-size 组成生产端批次，过期处理交给独立有界线程池。
     */
    @Scheduled(fixedDelayString = "${sale.dispatch-delay-ms:100}")
    public void dispatch() {
        for (var client : properties.clients()) {
            try {
                for (var batch : batches(preallocation.claim(client.id()))) {
                    var active = routeExpired(batch);
                    if (active.isEmpty()) { continue; }
                    try { publications.execute(() -> deliverRequests(active)); }
                    catch (RejectedExecutionException error) { defer(active); }
                }
            } catch (DataAccessException error) {
                log.warn("Redis pending requests unavailable for {}", client.id());
            }
        }
    }

    /** Redis 补偿独立调度，每次领取最多 64 条，按租户批量核对和同步后完成领取租约。 */
    @Scheduled(fixedDelayString = "${sale.redis-sync-delay-ms:50}")
    public void dispatchRedis() {
        var groups = new LinkedHashMap<String, List<OutboxService.Delivery>>();
        for (var event : outbox.claim(List.of("REDIS"), 64)) {
            groups.computeIfAbsent(event.tenant(), ignored -> new ArrayList<>()).add(event);
        }
        var tasks = new ArrayList<CompletableFuture<Void>>();
        for (var events : groups.values()) {
            tasks.add(CompletableFuture.runAsync(() -> deliverRedis(events), synchronizations));
        }
        CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new)).join();
    }

    /** 慢通知和旧 Rabbit 事件使用独立调度，保持原事件标识和失败退避。 */
    @Scheduled(fixedDelayString = "${sale.notification-delay-ms:100}")
    public void dispatchNotifications() {
        var tasks = outbox.claim(List.of("RABBIT", "WEBHOOK"), 16).stream()
                .map(event -> CompletableFuture.runAsync(() -> deliver(event), deliveries)).toList();
        CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new)).join();
    }

    /** 同租户输入按活动分组，输出不超过 sale.batch-size 的批次，保留组内领取顺序。 */
    private List<List<Preallocation.Claim>> batches(List<Preallocation.Claim> claims) {
        int size = properties.batchSize();
        var groups = new LinkedHashMap<String, List<Preallocation.Claim>>();
        for (var claim : claims) {
            groups.computeIfAbsent(claim.request().reservation().activityId(), ignored -> new ArrayList<>()).add(claim);
        }
        var batches = new ArrayList<List<Preallocation.Claim>>();
        for (var group : groups.values()) {
            for (int start = 0; start < group.size(); start += size) {
                batches.add(List.copyOf(group.subList(start, Math.min(group.size(), start + size))));
            }
        }
        return batches;
    }

    /** 调度和实际发布前均检查业务期限，Broker 故障与发布队列阻塞不会拖住到期清理。 */
    private List<Preallocation.Claim> routeExpired(List<Preallocation.Claim> claims) {
        long now = System.currentTimeMillis();
        var expired = claims.stream().filter(claim -> claim.request().reservation().queueDeadline().toEpochMilli() <= now).toList();
        var active = claims.stream().filter(claim -> claim.request().reservation().queueDeadline().toEpochMilli() > now).toList();
        if (!expired.isEmpty()) {
            try { expirations.execute(() -> expireRequests(expired)); }
            catch (RejectedExecutionException error) { defer(expired); }
        }
        return active;
    }

    /** 发布同活动小批次并等待确认，成功等待业务结果，失败按持久计数退避。 */
    private void deliverRequests(List<Preallocation.Claim> claims) {
        var active = routeExpired(claims);
        if (active.isEmpty()) { return; }
        try {
            var requests = active.stream().map(Preallocation.Claim::request).toList();
            String payload = json.writeValueAsString(requests.size() == 1 ? requests.getFirst() : requests);
            publish(active.getFirst().token() + ":" + requests.getFirst().requestId(), payload);
            preallocation.reschedule(active, true);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            defer(active);
        } catch (Exception error) {
            log.warn("Request batch delivery deferred: {}", error.getClass().getSimpleName());
            defer(active);
        }
    }

    /** 已到期申请在同活动小事务中落库并结束，技术故障继续保留 pending 等待退避恢复。 */
    private void expireRequests(List<Preallocation.Claim> claims) {
        try { sales.processBatch(claims.stream().map(Preallocation.Claim::request).toList()); }
        catch (Exception error) {
            log.warn("Pending expiry batch deferred: {}", error.getClass().getSimpleName());
            defer(claims);
        }
    }

    /** 仅持有当前令牌的投递者可以修改重试时间，Redis 故障时保留原租约恢复。 */
    private void defer(List<Preallocation.Claim> claims) {
        try { preallocation.reschedule(claims, false); }
        catch (DataAccessException error) { log.warn("Request retry scheduling deferred"); }
    }

    /** 批量读取已同步版本，缺失版本用一次 Lua 补齐，SQL 完成标记仍校验领取租约。 */
    private void deliverRedis(List<OutboxService.Delivery> events) {
        try {
            var results = events.stream().map(event -> json.readValue(event.payload(), ReservationView.class)).toList();
            preallocation.synchronizeMissing(events.getFirst().tenant(), results);
            outbox.delivered(events);
        } catch (Exception error) {
            log.warn("Redis result batch deferred: {}", error.getClass().getSimpleName());
            for (var event : events) { outbox.failed(event, error.getClass().getSimpleName()); }
        }
    }

    /** 固定工作线程和等待队列，容量耗尽时由调用方保留持久化任务并退避。 */
    private static ExecutorService executor(String name, int workers, int capacity) {
        return new ThreadPoolExecutor(workers, workers, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(capacity), Thread.ofVirtual().name(name, 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * 发送旧 Rabbit 事件或调用 Webhook，并记录投递结果。
     * 失败时登记重试或终止状态，中断时同时恢复线程的中断标记。
     *
     * @param event 已领取并携带租约的事件
     */
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

    /**
     * 应用销毁时关闭投递线程池，最多等待 5 秒，超时或被中断时强制停止剩余任务。
     * 等待被中断时恢复当前线程的中断标记。
     */
    @PreDestroy
    void stop() {
        var executors = List.of(publications, expirations, synchronizations, deliveries);
        executors.forEach(ExecutorService::shutdown);
        try {
            for (var executor : executors) {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) { executor.shutdownNow(); }
            }
        } catch (InterruptedException error) {
            executors.forEach(ExecutorService::shutdownNow);
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
