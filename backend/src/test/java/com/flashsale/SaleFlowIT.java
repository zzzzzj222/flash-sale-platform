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
import com.flashsale.messaging.OutboxDispatcher;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
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
    @Container static final MySQLContainer MYSQL = new MySQLContainer("mysql:8.4").withDatabaseName("flashsale");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:8.2")
            .withExposedPorts(6379).withCommand("redis-server", "--requirepass", SECRET);
    @Container static final GenericContainer<?> RABBIT = new GenericContainer<>("rabbitmq:4.2-management")
            .withEnv("RABBITMQ_DEFAULT_USER", "flashsale").withEnv("RABBITMQ_DEFAULT_PASS", SECRET).withExposedPorts(5672);

    record Hook(String eventId, String timestamp, String signature, String body) {}
    static final List<Hook> HOOKS = new CopyOnWriteArrayList<>();
    static final Set<String> FAIL_ONCE = ConcurrentHashMap.newKeySet();
    static final Set<String> FAILED = ConcurrentHashMap.newKeySet();
    static final HttpServer WEBHOOK = webhookServer();

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
    @Autowired SaleService sales;
    @Autowired RabbitTemplate rabbit;
    @Autowired SaleProperties properties;
    @Autowired RabbitListenerEndpointRegistry listeners;
    final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    @AfterAll static void closeWebhook() { WEBHOOK.stop(0); }

    record Response(int status, String body) {}

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

    ActivityView activity(String tenant, int stock, int limit, int holdSeconds) {
        var input = new CreateActivity("resource-" + UUID.randomUUID(), "集成测试活动", stock, limit, holdSeconds,
                60, Instant.now().minusSeconds(2), Instant.now().plusSeconds(600));
        var response = call(tenant, "POST", "/api/v1/activities", input);
        assertThat(response.status()).as(response.body()).isEqualTo(201);
        return json.readValue(response.body(), ActivityView.class);
    }

    Reserve request(ActivityView activity, String user) {
        return new Reserve(UUID.randomUUID(), UUID.fromString(activity.id()), user, 1);
    }

    void submit(String tenant, Reserve request) {
        var response = call(tenant, "POST", "/api/v1/requests", request);
        assertThat(response.status()).as(response.body()).isEqualTo(202);
    }

    ReservationView completed(String tenant, UUID id) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                .until(() -> !sales.result(tenant, id.toString()).state().equals("QUEUED"));
        return sales.result(tenant, id.toString());
    }

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
            store.jdbc().sql("UPDATE reservations SET queue_deadline_ms=? WHERE tenant_id='demo' AND request_id=?")
                    .params(store.now() - 1, timedOut.requestId().toString()).update();
            sales.expire("demo", timedOut.requestId().toString());
            sales.process("demo", cancelled.requestId().toString());
            sales.process("demo", timedOut.requestId().toString());
            assertThat(sales.result("demo", cancelled.requestId().toString()).state()).isEqualTo("RELEASED");
            assertThat(sales.result("demo", timedOut.requestId().toString()).reason()).isEqualTo("QUEUE_TIMEOUT");
            assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(2);
        } finally { listeners.start(); }
    }

    @Test
    void brokerOutageKeepsAcceptedRequestAndRecovers() {
        var activity = activity("demo", 2, 1, 120);
        var input = request(activity, "broker-recovery");
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(RABBIT.getContainerId()).exec();
        try {
            submit("demo", input);
            assertThat(sales.result("demo", input.requestId().toString()).state()).isEqualTo("QUEUED");
            await().atMost(Duration.ofSeconds(15)).until(() -> store.jdbc()
                    .sql("SELECT COUNT(*) FROM outbox WHERE tenant_id='demo' AND request_id=? AND last_error IS NOT NULL")
                    .param(input.requestId().toString()).query(Integer.class).single() == 1);
        } finally { docker.unpauseContainerCmd(RABBIT.getContainerId()).exec(); }
        assertThat(completed("demo", input.requestId()).state()).isEqualTo("RESERVED");
        assertThat(sales.activity("demo", activity.id()).availableStock()).isEqualTo(1);
    }

    @Test
    void redisOutageRejectsBeforeCreatingARequest() {
        var activity = activity("demo", 2, 1, 120);
        var input = request(activity, "redis-unavailable");
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            assertThat(call("demo", "POST", "/api/v1/requests", input).status()).isEqualTo(503);
            assertThat(store.find("demo", input.requestId().toString(), false)).isEmpty();
        } finally { docker.unpauseContainerCmd(REDIS.getContainerId()).exec(); }
    }

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
