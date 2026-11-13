package com.flashsale;

import java.net.URI;
import java.util.List;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * 秒杀业务的配置属性，绑定 {@code sale.*} 前缀的配置项。
 *
 * @param demoEnabled                是否启用演示模式
 * @param workersEnabled             是否启用后台异步 worker（队列写入、Webhook 通知等）
 * @param queueName                  排队使用的队列名称
 * @param queueLimit                 主队列最大消息条数，每条消息最多包含 16 个申请
 * @param batchSize                  生产端每条消息打包的申请数，1 表示请求逐条投递
 * @param requestPublishWaitMs       发布确认后首次等待结果的毫秒数
 * @param requestClaimLeaseMs        待处理申请领取租约的毫秒数
 * @param admissionGlobalPerSecond   全局每秒准入请求数
 * @param admissionClientPerSecond   单个客户端每秒准入请求数
 * @param admissionUserPerSecond     单个用户每秒准入请求数
 * @param clients                    已授权的客户端列表，客户端 ID 必须唯一
 */
@Validated
@ConfigurationProperties("sale")
public record SaleProperties(
        boolean demoEnabled,
        boolean workersEnabled,
        @NotBlank String queueName,
        @Min(1) int queueLimit,
        @Min(1) @Max(16) int batchSize,
        @Min(1000) @Max(60000) long requestPublishWaitMs,
        @Min(3000) @Max(300000) long requestClaimLeaseMs,
        @Min(1) int admissionGlobalPerSecond,
        @Min(1) int admissionClientPerSecond,
        @Min(1) int admissionUserPerSecond,
        @NotEmpty List<@Valid Client> clients) {

    /**
     * 校验客户端 ID 的唯一性，重复时直接拒绝启动。
     *
     * @throws IllegalArgumentException 客户端 ID 存在重复时抛出
     */
    public SaleProperties {
        if (clients != null && clients.stream().map(Client::id).distinct().count() != clients.size()) {
            throw new IllegalArgumentException("Client IDs must be unique");
        }
    }

    /**
     * 按 ID 查找已配置的客户端。
     *
     * @param id 客户端 ID
     * @return 匹配的客户端配置
     * @throws IllegalArgumentException 找不到对应客户端时抛出
     */
    public Client client(String id) {
        return clients.stream().filter(client -> client.id().equals(id)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown client"));
    }

    /**
     * 客户端配置，包含凭据、角色权限及可选的 Webhook 通知配置。
     *
     * @param id            客户端 ID，仅允许字母、数字、下划线和中划线，长度 1-64
     * @param secret        客户端密钥，长度至少 24 位
     * @param admin         是否具有管理员权限
     * @param operator      是否具有运营权限
     * @param webhookUrl    Webhook 回调地址，可为空
     * @param webhookSecret Webhook 签名密钥，配置 Webhook 时必填且长度至少 24 位
     */
    public record Client(
            @NotBlank @Pattern(regexp = "[a-zA-Z0-9_-]{1,64}") String id,
            @NotBlank @Size(min = 24) String secret,
            boolean admin,
            boolean operator,
            String webhookUrl,
            String webhookSecret) {

        /**
         * 校验 Webhook 配置的合法性：必须为 HTTP(S) 地址、禁止携带用户信息，并提供独立的签名密钥。
         *
         * @throws IllegalArgumentException Webhook 配置不合法时抛出
         */
        public Client {
            if (webhookUrl != null && !webhookUrl.isBlank()) {
                URI uri = URI.create(webhookUrl);
                if (!("https".equals(uri.getScheme()) || "http".equals(uri.getScheme()))
                        || uri.getHost() == null || uri.getUserInfo() != null
                        || webhookSecret == null || webhookSecret.length() < 24) {
                    throw new IllegalArgumentException("Webhook requires an HTTP(S) URL and a separate 24-character secret");
                }
            }
        }

        /**
         * 判断该客户端是否启用了 Webhook 通知。
         *
         * @return 配置了非空 Webhook 地址时返回 {@code true}
         */
        public boolean hasWebhook() {
            return webhookUrl != null && !webhookUrl.isBlank();
        }

        /**
         * 输出不含密钥的客户端信息，避免敏感数据被写入日志。
         *
         * @return 仅包含 ID 与管理员标记的字符串
         */
        @Override
        public String toString() {
            return "Client[id=" + id + ", admin=" + admin + "]";
        }
    }
}
