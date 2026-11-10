package com.flashsale.core;

import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import com.flashsale.api.ApiException;
import com.flashsale.api.Contracts.ActivityView;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.ObjectMapper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SaleStoreCacheTest {
    static final String ID = "b68c35e3-19df-4c78-92b8-1f804ee704f3";
    static final String KEY = "flashsale:activity:metadata:v1:demo:" + ID;
    static final String STOCK_SQL = "SELECT available_stock FROM activities WHERE tenant_id=? AND id=?";
    static final String METADATA_SQL = "SELECT * FROM activities WHERE tenant_id=:tenant AND id IN (:ids)";
    final JdbcClient jdbc = mock(JdbcClient.class, RETURNS_DEEP_STUBS);
    final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    final ValueOperations<String, String> values = mock(ValueOperations.class);
    final ObjectMapper json = new ObjectMapper();
    final SaleStore store = new SaleStore(jdbc, redis, json);
    final ActivityView activity = new ActivityView(ID, "resource", "测试活动", 10, 10, 2, 120, 60,
            Instant.parse("2026-11-09T00:00:00Z"), Instant.parse("2026-11-10T00:00:00Z"));

    void stubRead(String tenant) {
        when(redis.opsForValue()).thenReturn(values);
        when(values.multiGet(List.of("flashsale:activity:metadata:v1:" + tenant + ":" + ID)))
                .thenReturn(Collections.singletonList(null));
        when(jdbc.sql(STOCK_SQL).params(tenant, ID).query(Integer.class).optional()).thenReturn(Optional.of(10));
        when(jdbc.sql(METADATA_SQL).param("tenant", tenant).param("ids", List.of(ID))
                .query(org.mockito.ArgumentMatchers.<RowMapper<ActivityView>>any()).list()).thenReturn(List.of(activity));
    }

    @Test
    void cacheHitReusesRulesButReadsCurrentStock() {
        stubRead("demo");
        String cached = json.writeValueAsString(SaleStore.ActivityMetadata.from(activity));
        when(values.multiGet(List.of(KEY)))
                .thenReturn(Collections.singletonList(null))
                .thenReturn(List.of(cached));
        when(jdbc.sql(STOCK_SQL).params("demo", ID).query(Integer.class).optional())
                .thenReturn(Optional.of(10))
                .thenReturn(Optional.of(9));

        assertThat(store.activity("demo", ID, false)).isEqualTo(activity);
        var payload = ArgumentCaptor.forClass(String.class);
        var ttl = ArgumentCaptor.forClass(Duration.class);
        verify(values).set(eq(KEY), payload.capture(), ttl.capture());
        assertThat(json.readTree(payload.getValue()).has("availableStock")).isFalse();
        assertThat(ttl.getValue()).isBetween(Duration.ofSeconds(300), Duration.ofSeconds(360));

        clearInvocations(jdbc);
        var current = store.activity("demo", ID, false);
        assertThat(current.title()).isEqualTo(activity.title());
        assertThat(current.startsAt()).isEqualTo(activity.startsAt());
        assertThat(current.limitPerUser()).isEqualTo(2);
        assertThat(current.availableStock()).isEqualTo(9);
        verify(jdbc, never()).sql(METADATA_SQL);
    }

    @Test
    void redisReadFailureFallsBackWithoutRetryingWrites() {
        stubRead("demo");
        when(values.multiGet(anyList())).thenThrow(new RedisConnectionFailureException("unavailable"));
        assertThat(store.activity("demo", ID, false)).isEqualTo(activity);
        verify(values, never()).set(anyString(), anyString(), any(Duration.class));
    }

    @Test
    void redisWriteFailureStillReturnsDatabaseResult() {
        stubRead("demo");
        doThrow(new RedisConnectionFailureException("unavailable")).when(values).set(anyString(), anyString(), any(Duration.class));
        assertThat(store.activity("demo", ID, false)).isEqualTo(activity);
    }

    @Test
    void malformedCacheIsReloadedAndReplaced() {
        stubRead("demo");
        when(values.multiGet(List.of(KEY))).thenReturn(List.of("{invalid"));
        assertThat(store.activity("demo", ID, false)).isEqualTo(activity);
        verify(values).set(eq(KEY), eq(json.writeValueAsString(SaleStore.ActivityMetadata.from(activity))), any(Duration.class));
    }

    @Test
    void transactionsAndLockingReadsBypassCache() {
        for (boolean lock : List.of(false, true)) {
            String sql = "SELECT * FROM activities WHERE tenant_id=? AND id=?" + (lock ? " FOR UPDATE" : "");
            when(jdbc.sql(sql).params("demo", ID).query(org.mockito.ArgumentMatchers.<RowMapper<ActivityView>>any()).optional())
                    .thenReturn(Optional.of(activity));
        }
        when(jdbc.sql("SELECT * FROM activities WHERE tenant_id=? ORDER BY created_at_ms DESC LIMIT 100")
                .param("demo").query(org.mockito.ArgumentMatchers.<RowMapper<ActivityView>>any()).list()).thenReturn(List.of(activity));
        assertThat(store.activity("demo", ID, true)).isEqualTo(activity);
        TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            assertThat(store.activity("demo", ID, false)).isEqualTo(activity);
            assertThat(store.activities("demo")).containsExactly(activity);
        } finally {
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
        verifyNoInteractions(redis);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void activityListLoadsMissingRulesInOneBatch() throws Exception {
        stubRead("demo");
        var second = new ActivityView("second-id", "other-resource", "另一个活动", 20, 4, 1, 60, 30,
                activity.startsAt(), activity.endsAt());
        String listSql = "SELECT id,available_stock FROM activities WHERE tenant_id=? ORDER BY created_at_ms DESC LIMIT 100";
        var statement = mock(JdbcClient.StatementSpec.class, RETURNS_SELF);
        when(jdbc.sql(listSql)).thenReturn(statement);
        when(statement.query(any(RowMapper.class))).thenAnswer(invocation -> {
            RowMapper<?> mapper = invocation.getArgument(0);
            var firstRow = mock(ResultSet.class);
            when(firstRow.getString("id")).thenReturn(ID);
            when(firstRow.getInt("available_stock")).thenReturn(9);
            var secondRow = mock(ResultSet.class);
            when(secondRow.getString("id")).thenReturn(second.id());
            when(secondRow.getInt("available_stock")).thenReturn(4);
            var query = mock(JdbcClient.MappedQuerySpec.class);
            var rows = List.of(mapper.mapRow(firstRow, 0), mapper.mapRow(secondRow, 1));
            when(query.list()).thenReturn(rows);
            return query;
        });
        when(values.multiGet(List.of(KEY, "flashsale:activity:metadata:v1:demo:" + second.id())))
                .thenReturn(java.util.Arrays.asList(null, null));
        when(jdbc.sql(METADATA_SQL).param("tenant", "demo").param("ids", List.of(ID, second.id()))
                .query(org.mockito.ArgumentMatchers.<RowMapper<ActivityView>>any()).list()).thenReturn(List.of(activity, second));
        clearInvocations(jdbc);
        var result = store.activities("demo");
        assertThat(result).hasSize(2);
        assertThat(result.getFirst().availableStock()).isEqualTo(9);
        assertThat(result.get(1)).isEqualTo(second);
        verify(jdbc).sql(METADATA_SQL);
    }

    @Test
    void cacheKeysIncludeTenant() {
        stubRead("demo");
        stubRead("partner");
        store.activity("demo", ID, false);
        store.activity("partner", ID, false);
        verify(values).multiGet(List.of(KEY));
        verify(values).multiGet(List.of("flashsale:activity:metadata:v1:partner:" + ID));
    }

    @Test
    void missingActivityDoesNotReadOrPopulateCache() {
        when(jdbc.sql(STOCK_SQL).params("demo", ID).query(Integer.class).optional()).thenReturn(Optional.empty());
        assertThatThrownBy(() -> store.activity("demo", ID, false)).isInstanceOf(ApiException.class);
        verifyNoInteractions(redis);
    }
}
