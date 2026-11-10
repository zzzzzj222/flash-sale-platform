package com.flashsale;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import com.flashsale.api.Contracts.*;
import com.flashsale.core.SaleService;
import com.flashsale.core.SaleStore;
import com.flashsale.core.Preallocation;
import com.flashsale.messaging.OutboxDispatcher;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import com.github.dockerjava.api.model.ExposedPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import tools.jackson.databind.ObjectMapper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "sale.demo-enabled=false", "sale.admission-global-per-second=100000", "sale.admission-client-per-second=100000",
        "sale.admission-user-per-second=100000", "spring.rabbitmq.listener.simple.concurrency=4",
        "sale.clients[0].secret=" + SaleFlowIT.SECRET, "sale.clients[1].secret=" + SaleFlowIT.SECRET,
        "sale.clients[2].id=reader", "sale.clients[2].secret=" + SaleFlowIT.SECRET, "sale.clients[2].admin=false"
})
class SaleFlowIT {
    static final String SECRET = "integration-only-credential-1234567890";
    static final String HOOK_SECRET = "integration-webhook-secret-1234567890";
    @Container static final MySQLContainer MYSQL = mysqlContainer();
    @Container static final GenericContainer<?> REDIS = redisContainer();
    @Container static final GenericContainer<?> RABBIT = rabbitContainer();

    /**
     * 作用：配置集成测试使用的 MySQL 容器，数据库名为 flashsale。
     * @return 尚未启动、由 Testcontainers 管理生命周期的 MySQL 容器
     */
    static MySQLContainer mysqlContainer() {
        var container = new MySQLContainer("mysql:8.4");
        container.withDatabaseName("flashsale");
        return container;
    }

    /**
     * 作用：配置启用密码认证并暴露 6379 端口的 Redis 测试容器。
     * @return 尚未启动、由 Testcontainers 管理生命周期的 Redis 容器
     */
    static GenericContainer<?> redisContainer() {
        var container = new GenericContainer<>("redis:8.2");
        container.withExposedPorts(6379);
        container.withCommand("redis-server", "--appendonly", "yes", "--appendfsync", "always",
                "--maxmemory-policy", "noeviction", "--requirepass", SECRET);
        return container;
    }

    /**
     * 作用：配置测试账号并暴露 AMQP 端口的 RabbitMQ 测试容器。
     * @return 尚未启动、由 Testcontainers 管理生命周期的 RabbitMQ 容器
     */
    static GenericContainer<?> rabbitContainer() {
        var container = new GenericContainer<>("rabbitmq:4.2-management");
        container.withEnv("RABBITMQ_DEFAULT_USER", "flashsale");
        container.withEnv("RABBITMQ_DEFAULT_PASS", SECRET);
        container.withExposedPorts(5672);
        return container;
    }

    record Hook(String eventId, String timestamp, String signature, String body) {}
    static final List<Hook> HOOKS = new CopyOnWriteArrayList<>();
    static final Set<String> FAIL_ONCE = ConcurrentHashMap.newKeySet();
    static final Set<String> FAILED = ConcurrentHashMap.newKeySet();
    static final HttpServer WEBHOOK = webhookServer();

    /**
     * 作用：启动本地 Webhook 接收服务，记录事件头和正文；指定请求首次回调返回 503，其余返回 204。
     * @return 已启动、监听本机随机端口的 HTTP 服务
     * @throws IllegalStateException 服务创建失败时抛出
     */
    static HttpServer webhookServer() {
        try {
            var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/events", exchange -> {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                HOOKS.add(new Hook(exchange.getRequestHeaders().getFirst("X-Event-Id"),
                        exchange.getRequestHeaders().getFirst("X-Event-Timestamp"),
                        exchange.getRequestHeaders().getFirst("X-Signature"), body));
                boolean fail = FAIL_ONCE.stream().anyMatch(id -> body.contains(id) && FAILED.add(id));
                exchange.sendResponseHeaders(fail ? 503 : 204, -1);
                exchange.close();
            });
            server.start();
            return server;
        } catch (java.io.IOException error) { throw new IllegalStateException(error); }
    }

    /**
     * 作用：向 Spring 测试上下文注册客户端权限、容器连接信息、独立队列和 Webhook 配置。
     * @param registry 测试上下文的动态属性注册器
     */
    @DynamicPropertySource
    static void services(DynamicPropertyRegistry registry) {
        registry.add("sale.clients[0].id", () -> "demo");
        registry.add("sale.clients[0].secret", () -> SECRET);
        registry.add("sale.clients[0].admin", () -> true);
        registry.add("sale.clients[0].operator", () -> true);
        registry.add("sale.clients[1].id", () -> "partner");
        registry.add("sale.clients[1].secret", () -> SECRET);
        registry.add("sale.clients[1].admin", () -> true);
        registry.add("sale.clients[2].id", () -> "reader");
        registry.add("sale.clients[2].secret", () -> SECRET);
        registry.add("sale.clients[2].admin", () -> false);
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.data.redis.password", () -> SECRET);
        registry.add("spring.rabbitmq.host", RABBIT::getHost);
        registry.add("spring.rabbitmq.port", () -> RABBIT.getMappedPort(5672));
        registry.add("spring.rabbitmq.username", () -> "flashsale");
        registry.add("spring.rabbitmq.password", () -> SECRET);
        registry.add("sale.queue-name", () -> "flashsale.integration." + MYSQL.getContainerId().substring(0, 8));
        registry.add("sale.clients[0].webhook-url", () -> "http://127.0.0.1:" + WEBHOOK.getAddress().getPort() + "/events");
        registry.add("sale.clients[0].webhook-secret", () -> HOOK_SECRET);
    }

    @Value("${local.server.port}") int port;
    @Autowired ObjectMapper json;
    @Autowired SaleStore store;
    @Autowired StringRedisTemplate redis;
    @Autowired LettuceConnectionFactory redisConnections;
    @Autowired SaleService sales;
    @Autowired Preallocation preallocation;
    @Autowired RabbitTemplate rabbit;
    @Autowired SaleProperties properties;
    @Autowired RabbitListenerEndpointRegistry listeners;
    final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    /**
     * 作用：在本类全部测试结束后停止本地 Webhook 接收服务。
     */
    @AfterAll static void closeWebhook() { WEBHOOK.stop(0); }

    record Response(int status, String body) {}

    /**
     * 作用：向测试应用发送 HTTP 请求，按需添加 Basic 认证并将请求体序列化为 JSON。
     * @param tenant 客户端标识，传 null 时不携带认证信息
     * @param method HTTP 请求方法
     * @param path 以斜杠开头的接口路径
     * @param body 请求体对象，传 null 时发送空请求体
     * @return 包含 HTTP 状态码和响应正文的结果
     * @throws IllegalStateException 请求构建、序列化或发送失败时抛出
     */
    Response call(String tenant, String method, String path, Object body) {
        try {
            var builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).timeout(Duration.ofSeconds(20));
            if (tenant != null) {
                builder.header("Authorization", "Basic " + Base64.getEncoder()
                        .encodeToString((tenant + ":" + SECRET).getBytes(StandardCharsets.UTF_8)));
            }
            if (body == null) builder.method(method, HttpRequest.BodyPublishers.noBody());
            else builder.header("Content-Type", "application/json").method(method,
                    HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body), StandardCharsets.UTF_8));
            var response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            return new Response(response.statusCode(), response.body());
        } catch (Exception error) { throw new IllegalStateException(error); }
    }

    /**
     * 作用：通过接口创建当前可参与的测试活动，断言创建成功并解析活动信息。
     * @param tenant 创建活动的客户端标识
     * @param stock 活动初始库存
     * @param limit 每个用户的限购数量
     * @param holdSeconds 预占有效期，单位为秒
     * @return 创建成功的活动信息
     */
    ActivityView activity(String tenant, int stock, int limit, int holdSeconds) {
        var input = new CreateActivity("resource-" + UUID.randomUUID(), "集成测试活动", stock, limit, holdSeconds,
                60, Instant.now().minusSeconds(2), Instant.now().plusSeconds(600));
        var response = call(tenant, "POST", "/api/v1/activities", input);
        assertThat(response.status()).as(response.body()).isEqualTo(201);
        return json.readValue(response.body(), ActivityView.class);
    }

    /**
     * 作用：为指定活动和用户构造购买数量为 1、请求标识随机生成的预占请求。
     * @param activity 参与预占的活动
     * @param user 用户标识
     * @return 尚未提交的预占请求
     */
    Reserve request(ActivityView activity, String user) {
        return new Reserve(UUID.randomUUID(), UUID.fromString(activity.id()), user, 1);
    }

    /**
     * 作用：通过接口提交预占请求，并断言返回 202，表示请求已被接受。
     * @param tenant 提交请求的客户端标识
     * @param request 待提交的预占请求
     */
    void submit(String tenant, Reserve request) {
        var response = call(tenant, "POST", "/api/v1/requests", request);
        assertThat(response.status()).as(response.body()).isEqualTo(202);
    }

    /**
     * 作用：每 100 毫秒查询一次请求状态，最多等待 30 秒，直到请求离开 QUEUED 状态。
     * @param tenant 请求所属的客户端标识
     * @param id 预占请求标识
     * @return 离开排队状态后再次查询到的预占结果，不保证预占成功
     */
    ReservationView completed(String tenant, UUID id) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                .until(() -> !sales.result(tenant, id.toString()).state().equals("QUEUED"));
        return sales.result(tenant, id.toString());
    }

    /**
     * 作用：验证 64 个用户并发抢购 10 件库存时恰好成功 10 次，失败 54 次，库存和已用配额一致。
     * @throws Exception 等待并发任务时发生中断或任务执行失败时抛出
     */
    @Test
    void sixtyFourConcurrentBuyersCannotOversellTenUnits() throws Exception {
        var activity = activity("demo", 10, 1, 120);
        var inputs = new ArrayList<Reserve>();
        for (int index = 0; index < 64; index++) inputs.add(request(activity, "buyer-" + index));
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = inputs.stream().map(input -> executor.submit(() -> submit("demo", input))).toList();
            for (var future : futures) future.get();
        }
        var results = inputs.stream().map(input -> completed("demo", input.requestId())).toList();
        assertThat(results.stream().filter(item -> item.state().equals("RESERVED")).count()).isEqualTo(10);
        assertThat(results.stream().filter(item -> item.state().equals("REJECTED")).count()).isEqualTo(54);
        assertThat(sales.activity("demo", activity.id()).availableStock()).isZero();
        assertThat(store.jdbc().sql("SELECT SUM(used_quantity) FROM user_quotas WHERE tenant_id='demo' AND activity_id=?")
                .param(activity.id()).query(Integer.class).single()).isEqualTo(10);
    }

    /**
     * 作用：验证同一请求并发提交、消息重复投递及再次处理时，只扣减一次库存并保留一条预占记录。
     * @throws Exception 等待并发任务时发生中断或任务执行失败时抛出
     */
    @Test
    void duplicateConcurrentSubmissionsAndDuplicateMessagesDeductOnce() throws Exception {
        var activity = activity("demo", 10, 2, 120);
        var input = request(activity, "duplicate-user");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = java.util.stream.IntStream.range(0, 12)
                    .mapToObj(index -> executor.submit(() -> submit("demo", input))).toList();
            for (var future : futures) future.get();
        }
        assertThat(completed("demo", input.requestId()).state()).isEqualTo("RESERVED");
        for (int index = 0; index < 4; index++) {
            rabbit.convertAndSend("", properties.queueName(), json.writeValueAsString(new RequestMessage("demo", input.requestId().toString())));
        }
        sales.process("demo", input.requestId().toString());
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(9);
        assertThat(store.jdbc().sql("SELECT COUNT(*) FROM reservations WHERE tenant_id='demo' AND request_id=?")
                .param(input.requestId().toString()).query(Integer.class).single()).isEqualTo(1);
    }

    /**
     * 作用：验证同一用户并发提交 12 个不同请求时，最多成功预占限购允许的 2 件库存。
     * @throws Exception 等待并发任务时发生中断或任务执行失败时抛出
     */
    @Test
    void concurrentRequestsFromOneUserRespectLimit() throws Exception {
        var activity = activity("demo", 20, 2, 120);
        var inputs = java.util.stream.IntStream.range(0, 12).mapToObj(index -> request(activity, "same-user")).toList();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var future : inputs.stream().map(input -> executor.submit(() -> submit("demo", input))).toList()) future.get();
        }
        var results = inputs.stream().map(input -> completed("demo", input.requestId())).toList();
        assertThat(results.stream().filter(item -> item.state().equals("RESERVED")).count()).isEqualTo(2);
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(18);
    }

    /**
     * 作用：验证仅大小写不同的用户标识分别计算限购额度，两个用户均可成功预占。
     */
    @Test
    void userIdentifiersRemainCaseSensitiveRegardlessOfDatabaseDefaults() {
        var activity = activity("demo", 2, 1, 120);
        var upper = request(activity, "User-A");
        var lower = request(activity, "user-a");
        submit("demo", upper);
        submit("demo", lower);
        assertThat(completed("demo", upper.requestId()).state()).isEqualTo("RESERVED");
        assertThat(completed("demo", lower.requestId()).state()).isEqualTo("RESERVED");
    }

    /**
     * 作用：验证客户端间的活动和请求隔离、未认证访问拦截，以及活动创建和监控接口的角色权限。
     */
    @Test
    void clientsAreIsolatedAndRolesAreEnforced() {
        var activity = activity("demo", 2, 1, 120);
        assertThat(call("partner", "GET", "/api/v1/activities/" + activity.id(), null).status()).isEqualTo(404);
        assertThat(call(null, "GET", "/api/v1/activities", null).status()).isEqualTo(401);
        assertThat(call("demo", "GET", "/openapi.yaml", null).body()).contains("openapi: 3.1.0");
        assertThat(call("partner", "GET", "/actuator/prometheus", null).status()).isEqualTo(403);
        assertThat(call("demo", "GET", "/actuator/prometheus", null).status()).isEqualTo(200);
        assertThat(call("reader", "POST", "/api/v1/activities", new CreateActivity("r", "t", 1, 1, 10, 10,
                Instant.now(), Instant.now().plusSeconds(60))).status()).isEqualTo(403);
        var input = request(activity, "private-user");
        submit("demo", input);
        assertThat(call("partner", "GET", "/api/v1/requests/" + input.requestId(), null).status()).isEqualTo(404);
        assertThat(call(null, "GET", "/demo/api/activities", null).status()).isEqualTo(401);
        var partnerActivity = activity("partner", 2, 1, 120);
        var partnerInput = request(partnerActivity, "private-user");
        submit("partner", partnerInput);
        assertThat(completed("partner", partnerInput.requestId()).state()).isEqualTo("RESERVED");
    }

    /**
     * 作用：验证缓存只保存活动规则，预占和释放后的库存仍实时读取，且缓存按客户端隔离。
     */
    @Test
    void cachedActivityRulesKeepStockLiveAcrossReservationAndRelease() {
        var activity = activity("demo", 2, 1, 120);
        String key = "flashsale:activity:metadata:v1:demo:" + activity.id();
        assertThat(redis.hasKey(key)).isFalse();
        assertThat(sales.activities("demo")).anySatisfy(item -> assertThat(item).isEqualTo(activity));
        String cached = redis.opsForValue().get(key);
        var metadata = json.readTree(cached);
        assertThat(metadata.get("title").asText()).isEqualTo(activity.title());
        assertThat(metadata.get("limitPerUser").asInt()).isEqualTo(1);
        assertThat(metadata.has("startsAt")).isTrue();
        assertThat(metadata.has("availableStock")).isFalse();
        assertThat(redis.getExpire(key)).isBetween(1L, 360L);
        assertThat(sales.activity("demo", activity.id())).isEqualTo(activity);

        var input = request(activity, "cached-stock-user");
        submit("demo", input);
        assertThat(completed("demo", input.requestId()).state()).isEqualTo("RESERVED");
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(1);
        assertThat(sales.activities("demo")).anySatisfy(item -> {
            assertThat(item.id()).isEqualTo(activity.id());
            assertThat(item.availableStock()).isEqualTo(1);
        });
        assertThat(call("demo", "POST", "/api/v1/requests/" + input.requestId() + "/release", null).status()).isEqualTo(200);
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(2);
        assertThat(redis.opsForValue().get(key)).isEqualTo(cached);
        assertThat(call("partner", "GET", "/api/v1/activities/" + activity.id(), null).status()).isEqualTo(404);
        assertThat(redis.hasKey("flashsale:activity:metadata:v1:partner:" + activity.id())).isFalse();
    }

    /**
     * 作用：验证活动缓存缺失、内容损坏或过期时，查询会重建缓存并恢复有效期。
     */
    @Test
    void activityCacheMissesAndInvalidEntriesAreRebuilt() {
        var first = activity("demo", 2, 1, 120);
        var second = activity("demo", 3, 2, 120);
        String firstKey = "flashsale:activity:metadata:v1:demo:" + first.id();
        String secondKey = "flashsale:activity:metadata:v1:demo:" + second.id();
        sales.activities("demo");
        redis.delete(firstKey);
        redis.opsForValue().set(secondKey, "{invalid", Duration.ofMinutes(1));
        assertThat(sales.activities("demo")).contains(first, second);
        assertThat(json.readTree(redis.opsForValue().get(firstKey)).get("id").asText()).isEqualTo(first.id());
        assertThat(json.readTree(redis.opsForValue().get(secondKey)).get("id").asText()).isEqualTo(second.id());

        redis.expire(firstKey, Duration.ofMillis(1));
        await().atMost(Duration.ofSeconds(5)).until(() -> !redis.hasKey(firstKey));
        assertThat(sales.activity("demo", first.id())).isEqualTo(first);
        assertThat(redis.getExpire(firstKey)).isBetween(1L, 360L);
    }

    /**
     * 作用：验证同一订单重复确认成功，更换订单号或释放已确认预占返回冲突，且不额外扣减库存。
     */
    @Test
    void confirmationIsIdempotentAndCannotBeCancelled() {
        var activity = activity("demo", 2, 1, 120);
        var input = request(activity, "paid-user");
        submit("demo", input);
        completed("demo", input.requestId());
        String path = "/api/v1/requests/" + input.requestId();
        assertThat(call("demo", "POST", path + "/confirm", new Confirm("order-123")).status()).isEqualTo(200);
        assertThat(call("demo", "POST", path + "/confirm", new Confirm("order-123")).status()).isEqualTo(200);
        assertThat(call("demo", "POST", path + "/confirm", new Confirm("order-456")).status()).isEqualTo(409);
        assertThat(call("demo", "POST", path + "/release", null).status()).isEqualTo(409);
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(1);
    }

    /**
     * 作用：验证预占超时后自动恢复库存和用户额度，使同一用户能够再次预占。
     */
    @Test
    void expiryRestoresStockAndQuota() {
        var activity = activity("demo", 1, 1, 1);
        var input = request(activity, "expiring-user");
        submit("demo", input);
        await().atMost(Duration.ofSeconds(15)).until(() -> sales.result("demo", input.requestId().toString()).state().equals("EXPIRED"));
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(1);
        var again = request(activity, "expiring-user");
        submit("demo", again);
        assertThat(completed("demo", again.requestId()).state()).isEqualTo("RESERVED");
    }

    /**
     * 作用：验证超过预占期限的确认请求返回冲突，同时将预占转为已过期并恢复库存。
     */
    @Test
    void latePaymentCannotConfirmAnExpiredHold() {
        var activity = activity("demo", 1, 1, 120);
        var input = request(activity, "late-user");
        submit("demo", input);
        completed("demo", input.requestId());
        store.jdbc().sql("UPDATE reservations SET expires_at_ms=? WHERE tenant_id='demo' AND request_id=?")
                .params(store.now() - 1, input.requestId().toString()).update();
        assertThat(call("demo", "POST", "/api/v1/requests/" + input.requestId() + "/confirm", new Confirm("late-order")).status()).isEqualTo(409);
        assertThat(sales.result("demo", input.requestId().toString()).state()).isEqualTo("EXPIRED");
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(1);
    }

    /**
     * 作用：暂停消费以构造排队场景，验证取消或排队超时的请求在后续处理时不会扣减库存。
     */
    @Test
    void queuedCancellationAndTimeoutCannotDeductLater() {
        listeners.stop();
        try {
            var activity = activity("demo", 2, 1, 120);
            var cancelled = request(activity, "queued-cancel");
            submit("demo", cancelled);
            assertThat(call("demo", "POST", "/api/v1/requests/" + cancelled.requestId() + "/release", null).status()).isEqualTo(200);
            var timedOut = request(activity, "queued-timeout");
            submit("demo", timedOut);
            String stateKey = "flashsale:preallocation:{demo}:state";
            var pending = (tools.jackson.databind.node.ObjectNode) json.readTree(
                    (String) redis.opsForHash().get(stateKey, "r:" + timedOut.requestId()));
            pending.put("queueDeadlineMs", store.now() - 1);
            redis.opsForHash().put(stateKey, "r:" + timedOut.requestId(), json.writeValueAsString(pending));
            sales.expire("demo", timedOut.requestId().toString());
            sales.process("demo", cancelled.requestId().toString());
            sales.process("demo", timedOut.requestId().toString());
            assertThat(sales.result("demo", cancelled.requestId().toString()).state()).isEqualTo("RELEASED");
            assertThat(sales.result("demo", timedOut.requestId().toString()).reason()).isEqualTo("QUEUE_TIMEOUT");
            assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(2);
        } finally { listeners.start(); }
    }

    /**
     * 作用：暂停 RabbitMQ，验证申请与预扣保留在 Redis，数据库异步落库，恢复后可成功预占。
     */
    @Test
    void brokerOutageKeepsAcceptedRequestAndRecovers() {
        var activity = activity("demo", 2, 1, 120);
        var input = request(activity, "broker-recovery");
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(RABBIT.getContainerId()).exec();
        try {
            submit("demo", input);
            assertThat(sales.result("demo", input.requestId().toString()).state()).isEqualTo("QUEUED");
            assertThat(store.find("demo", input.requestId().toString(), false)).isEmpty();
            assertThat(redis.opsForHash().get("flashsale:preallocation:{demo}:state", "stock:" + activity.id())).isEqualTo("1");
            assertThat(redis.opsForZSet().score("flashsale:preallocation:{demo}:pending", input.requestId().toString())).isNotNull();
        } finally { docker.unpauseContainerCmd(RABBIT.getContainerId()).exec(); }
        assertThat(completed("demo", input.requestId()).state()).isEqualTo("RESERVED");
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(1);
    }

    @Test
    void pendingRequestsSurviveRedisRestartAndDoNotNeedMysqlOnWarmSubmit() {
        var activity = activity("partner", 3, 2, 120);
        var first = request(activity, "durable-pending");
        preallocation.submit("partner", first);
        listeners.stop();
        var docker = DockerClientFactory.instance().client();
        try {
            var second = request(activity, "no-mysql-submit");
            docker.pauseContainerCmd(MYSQL.getContainerId()).exec();
            try {
                var accepted = preallocation.submit("partner", second);
                assertThat(accepted.state()).isEqualTo("QUEUED");
                assertThat(preallocation.submit("partner", second)).isEqualTo(accepted);
            } finally { docker.unpauseContainerCmd(MYSQL.getContainerId()).exec(); }
            docker.restartContainerCmd(REDIS.getContainerId()).exec();
            // Docker Desktop 重启随机端口容器时会重新分配宿主端口。
            var bindings = docker.inspectContainerCmd(REDIS.getContainerId()).exec().getNetworkSettings().getPorts();
            int restartedPort = Integer.parseInt(bindings.getBindings().get(ExposedPort.tcp(6379))[0].getHostPortSpec());
            redisConnections.stop();
            redisConnections.getStandaloneConfiguration().setPort(restartedPort);
            redisConnections.start();
            await().atMost(Duration.ofSeconds(20)).ignoreExceptions().until(() -> redis.hasKey("flashsale:preallocation:{partner}:state"));
            assertThat(preallocation.find("partner", second.requestId().toString())).isPresent();
            assertThat(redis.opsForHash().get("flashsale:preallocation:{partner}:state", "stock:" + activity.id())).isEqualTo("1");
            listeners.start();
            assertThat(completed("partner", second.requestId()).state()).isEqualTo("RESERVED");
        } finally { listeners.start(); }
    }

    @Test
    void redisCompensationIsRetriedAfterCommittedRelease() {
        var activity = activity("demo", 1, 1, 120);
        var input = request(activity, "compensated-user");
        submit("demo", input);
        assertThat(completed("demo", input.requestId()).state()).isEqualTo("RESERVED");
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            assertThat(call("demo", "POST", "/api/v1/requests/" + input.requestId() + "/release", null).status()).isEqualTo(200);
            assertThat(store.reservation("demo", input.requestId().toString(), false).state()).isEqualTo("RELEASED");
        } finally { docker.unpauseContainerCmd(REDIS.getContainerId()).exec(); }
        await().atMost(Duration.ofSeconds(20)).until(() -> "1".equals(redis.opsForHash()
                .get("flashsale:preallocation:{demo}:state", "stock:" + activity.id())));
        var again = request(activity, "compensated-user");
        submit("demo", again);
        assertThat(completed("demo", again.requestId()).state()).isEqualTo("RESERVED");
    }

    @Test
    void legacyReservationsAndQuotasAreImportedBeforeAdmission() {
        var activity = sales.create("reader", new CreateActivity("legacy", "旧版数据迁移", 3, 2, 120, 60,
                Instant.now().minusSeconds(2), Instant.now().plusSeconds(600)));
        String legacyId = UUID.randomUUID().toString();
        long now = store.now();
        store.jdbc().sql("UPDATE activities SET available_stock=2 WHERE tenant_id='reader' AND id=?").param(activity.id()).update();
        store.jdbc().sql("INSERT INTO user_quotas VALUES ('reader',?,'legacy-user',1)").param(activity.id()).update();
        store.jdbc().sql("""
                INSERT INTO reservations (tenant_id,request_id,activity_id,user_id,quantity,state,
                queue_deadline_ms,expires_at_ms,created_at_ms,updated_at_ms)
                VALUES ('reader',?,?,'legacy-user',1,'RESERVED',?,?,?,?)
                """).params(legacyId, activity.id(), now + 60000, now + 120000, now, now).update();
        var original = new Reserve(UUID.fromString(legacyId), UUID.fromString(activity.id()), "legacy-user", 1);
        assertThat(preallocation.submit("reader", original).state()).isEqualTo("RESERVED");
        var overLimit = new Reserve(UUID.randomUUID(), UUID.fromString(activity.id()), "legacy-user", 2);
        assertThat(preallocation.submit("reader", overLimit).reason()).isEqualTo("LIMIT_EXCEEDED");
        var allowed = new Reserve(UUID.randomUUID(), UUID.fromString(activity.id()), "new-user", 2);
        assertThat(preallocation.submit("reader", allowed).state()).isEqualTo("QUEUED");
        assertThat(completed("reader", allowed.requestId()).state()).isEqualTo("RESERVED");
        assertThat(sales.activity("reader", activity.id()).availableStock()).isZero();
        assertThat(call("reader", "POST", "/api/v1/requests", new Reserve(original.requestId(), original.activityId(), "other-user", 1)).status())
                .isEqualTo(409);
    }

    @Test
    void olderRedisEventsCannotReacquireReleasedStock() {
        var activity = activity("demo", 1, 1, 120);
        var input = request(activity, "ordered-events");
        submit("demo", input);
        var reserved = completed("demo", input.requestId());
        sales.release("demo", input.requestId().toString());
        preallocation.synchronize("demo", reserved);
        preallocation.synchronize("demo", reserved);
        assertThat(preallocation.find("demo", input.requestId().toString()).orElseThrow().state()).isEqualTo("RELEASED");
        assertThat(redis.opsForHash().get("flashsale:preallocation:{demo}:state", "stock:" + activity.id())).isEqualTo("1");
    }

    @Test
    void pendingRequestExpiresAndRefundsEvenWhileBrokerIsUnavailable() {
        var activity = sales.create("partner", new CreateActivity("queue-expiry", "排队超时补偿", 1, 1, 120, 1,
                Instant.now().minusSeconds(2), Instant.now().plusSeconds(600)));
        var input = request(activity, "broker-timeout");
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(RABBIT.getContainerId()).exec();
        try {
            submit("partner", input);
            var result = completed("partner", input.requestId());
            assertThat(result.state()).isEqualTo("REJECTED");
            assertThat(result.reason()).isEqualTo("QUEUE_TIMEOUT");
            assertThat(redis.opsForHash().get("flashsale:preallocation:{partner}:state", "stock:" + activity.id())).isEqualTo("1");
        } finally { docker.unpauseContainerCmd(RABBIT.getContainerId()).exec(); }
    }

    /**
     * 作用：暂停 Redis，验证活动查询仍可用，而预占提交返回 503 且不创建请求记录。
     */
    @Test
    void redisOutageRejectsBeforeCreatingARequest() {
        var activity = activity("demo", 2, 1, 120);
        var input = request(activity, "redis-unavailable");
        sales.activity("demo", activity.id());
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            assertThat(call("demo", "GET", "/api/v1/activities/" + activity.id(), null).status()).isEqualTo(200);
            assertThat(call("demo", "GET", "/api/v1/activities", null).status()).isEqualTo(200);
            assertThat(call("demo", "POST", "/api/v1/requests", input).status()).isEqualTo(503);
            assertThat(store.find("demo", input.requestId().toString(), false)).isEmpty();
        } finally { docker.unpauseContainerCmd(REDIS.getContainerId()).exec(); }
    }

    /**
     * 作用：模拟首次 Webhook 回调失败，验证重试保留事件标识且签名有效，并验证失败事件可由所属客户端重放。
     */
    @Test
    void webhookRetriesKeepEventIdAndValidSignatureAndCanBeReplayed() {
        var activity = activity("demo", 2, 1, 120);
        var input = request(activity, "webhook-user");
        FAIL_ONCE.add(input.requestId().toString());
        submit("demo", input);
        completed("demo", input.requestId());
        await().atMost(Duration.ofSeconds(30)).until(() -> HOOKS.stream().filter(hook -> hook.body().contains(input.requestId().toString())).count() >= 2);
        var hooks = HOOKS.stream().filter(hook -> hook.body().contains(input.requestId().toString())).toList();
        assertThat(hooks.stream().map(Hook::eventId).distinct().count()).isEqualTo(1);
        for (var hook : hooks) {
            assertThat(hook.signature()).isEqualTo("sha256=" + OutboxDispatcher.sign(HOOK_SECRET,
                    hook.eventId() + "." + hook.timestamp() + "." + hook.body()));
        }
        String eventId = hooks.getFirst().eventId();
        await().atMost(Duration.ofSeconds(10)).until(() -> store.jdbc().sql("SELECT status FROM outbox WHERE id=?").param(eventId).query(String.class).single().equals("DELIVERED"));
        store.jdbc().sql("UPDATE outbox SET status='DEAD',attempts=12 WHERE id=?").param(eventId).update();
        assertThat(call("demo", "GET", "/api/v1/events/failed", null).body()).contains(eventId);
        assertThat(call("partner", "POST", "/api/v1/events/" + eventId + "/retry", null).status()).isEqualTo(404);
        assertThat(call("demo", "POST", "/api/v1/events/" + eventId + "/retry", null).status()).isEqualTo(204);
        await().atMost(Duration.ofSeconds(10)).until(() -> HOOKS.stream().filter(hook -> hook.eventId().equals(eventId)).count() >= 3);
    }

    /**
     * 作用：验证无效入参返回 400，同一请求标识更改购买数量返回 409，原请求数量和库存扣减保持不变。
     */
    @Test
    void invalidInputAndChangedIdempotencyPayloadAreRejected() {
        assertThat(call("demo", "POST", "/api/v1/requests", java.util.Map.of("quantity", -1)).status()).isEqualTo(400);
        var activity = activity("demo", 3, 2, 120);
        var input = request(activity, "immutable-user");
        submit("demo", input);
        var changed = new Reserve(input.requestId(), input.activityId(), input.userId(), 2);
        assertThat(call("demo", "POST", "/api/v1/requests", changed).status()).isEqualTo(409);
        assertThat(completed("demo", input.requestId()).quantity()).isEqualTo(1);
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(2);
    }
}
