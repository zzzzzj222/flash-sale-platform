package com.flashsale.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import com.flashsale.SaleProperties;
import com.flashsale.api.ApiException;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class AdmissionGate {
    private static final DefaultRedisScript<Long> LIMIT = new DefaultRedisScript<>("""
            local count = redis.call('INCR', KEYS[1])
            if count == 1 then redis.call('PEXPIRE', KEYS[1], 1000) end
            if count > tonumber(ARGV[1]) then return 0 end
            return 1
            """, Long.class);
    private final StringRedisTemplate redis;
    private final SaleProperties properties;

    /**
     * 初始化 Redis 限流器及用户、租户和全局的每秒请求上限配置。
     */
    public AdmissionGate(StringRedisTemplate redis, SaleProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    /**
     * 依次检查租户内用户、租户整体和全局的请求频率。
     * 超限时抛出 429 异常，Redis 访问失败时抛出 503 异常。
     */
    public void check(String tenant, String user) {
        try {
            allow("user:" + tenant + ":" + digest(user), properties.admissionUserPerSecond());
            allow("client:" + tenant, properties.admissionClientPerSecond());
            allow("global", properties.admissionGlobalPerSecond());
        } catch (DataAccessException error) {
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "ADMISSION_UNAVAILABLE", "流量控制暂时不可用，请稍后重试");
        }
    }

    /**
     * 使用 Redis 脚本原子递增计数，并在首次计数时设置一秒有效期，超限时拒绝请求。
     *
     * @param key 不含公共前缀的限流维度标识
     * @param maximum 该计数窗口内允许的最大请求数
     */
    private void allow(String key, int maximum) {
        Long result = redis.execute(LIMIT, List.of("flashsale:admission:" + key), Integer.toString(maximum));
        if (!Long.valueOf(1).equals(result)) {
            throw new ApiException(HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMITED", "请求过于频繁，请稍后使用原请求编号重试");
        }
    }

    /**
     * 将字符串按 UTF-8 编码计算 SHA-256 摘要，返回用于 Redis 键的十六进制标识。
     */
    private static String digest(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }
}
