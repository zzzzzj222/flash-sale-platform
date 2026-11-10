package com.flashsale.api;

import java.time.Instant;
import java.util.UUID;
import jakarta.validation.constraints.*;

/**
 * 集中定义接口请求、响应及异步消息使用的数据结构。
 */
public final class Contracts {
    /**
     * 禁止实例化仅用于组织数据结构的容器类。
     */
    private Contracts() {}

    /**
     * 创建秒杀活动的请求参数。
     *
     * @param resourceId 资源标识，不能为空，最多 128 个字符
     * @param title 活动标题，不能为空，最多 120 个字符
     * @param stock 活动总库存，范围为 1 至 1000000000
     * @param limitPerUser 每个用户的限购数量，范围为 1 至 10000
     * @param holdSeconds 库存预占有效秒数，范围为 1 至 86400
     * @param queueTimeoutSeconds 排队超时秒数，范围为 1 至 3600
     * @param startsAt 活动开始时间，不能为空
     * @param endsAt 活动结束时间，不能为空
     */
    public record CreateActivity(
            @NotBlank @Size(max = 128) String resourceId,
            @NotBlank @Size(max = 120) String title,
            @Min(1) @Max(1000000000) int stock,
            @Min(1) @Max(10000) int limitPerUser,
            @Min(1) @Max(86400) int holdSeconds,
            @Min(1) @Max(3600) int queueTimeoutSeconds,
            @NotNull Instant startsAt,
            @NotNull Instant endsAt) {}

    /**
     * 提交库存预占的请求参数。
     *
     * @param requestId 请求编号，用于幂等处理，不能为空
     * @param activityId 活动编号，不能为空
     * @param userId 用户标识，不能为空，最多 128 个字符
     * @param quantity 申请预占的数量，范围为 1 至 10000
     */
    public record Reserve(
            @NotNull UUID requestId,
            @NotNull UUID activityId,
            @NotBlank @Size(max = 128) String userId,
            @Min(1) @Max(10000) int quantity) {}

    /**
     * 确认库存预占的请求参数。
     *
     * @param orderId 关联的订单编号，不能为空，最多 128 个字符
     */
    public record Confirm(@NotBlank @Size(max = 128) String orderId) {}

    /**
     * 返回活动信息及当前可用库存。
     *
     * @param id 活动编号
     * @param resourceId 资源标识
     * @param title 活动标题
     * @param stock 活动总库存
     * @param availableStock 当前可用库存
     * @param limitPerUser 每个用户的限购数量
     * @param holdSeconds 库存预占有效秒数
     * @param queueTimeoutSeconds 排队超时秒数
     * @param startsAt 活动开始时间
     * @param endsAt 活动结束时间
     */
    public record ActivityView(String id, String resourceId, String title, int stock, int availableStock,
                               int limitPerUser, int holdSeconds, int queueTimeoutSeconds,
                               Instant startsAt, Instant endsAt) {}

    /**
     * 返回预占请求的处理状态、关联订单和时间信息。
     *
     * @param requestId 请求编号
     * @param activityId 活动编号
     * @param userId 用户标识
     * @param quantity 申请预占的数量
     * @param state 当前预占状态
     * @param reason 状态原因
     * @param orderId 关联的订单编号
     * @param queueDeadline 排队截止时间
     * @param expiresAt 库存预占到期时间，尚未设置时为 {@code null}
     * @param createdAt 请求创建时间
     * @param updatedAt 请求最近更新时间
     */
    public record ReservationView(String requestId, String activityId, String userId, int quantity,
                                  String state, String reason, String orderId, Instant queueDeadline,
                                  Instant expiresAt, Instant createdAt, Instant updatedAt) {}

    /**
     * 传递待落库的完整申请，保留旧版仅包含定位信息的消息兼容性。
     *
     * @param tenantId 请求所属的租户标识
     * @param requestId 预占请求编号
     * @param reservation 预扣申请快照，旧版消息为 null
     */
    public record RequestMessage(String tenantId, String requestId, ReservationView reservation) {
        public RequestMessage(String tenantId, String requestId) { this(tenantId, requestId, null); }
    }

    /**
     * 携带预占状态变化信息的业务事件，用于 Webhook 通知。
     *
     * @param eventId 事件编号
     * @param type 事件类型
     * @param occurredAt 事件发生时间
     * @param reservation 事件对应的预占信息快照
     */
    public record BusinessEvent(String eventId, String type, Instant occurredAt, ReservationView reservation) {}

    /**
     * 返回异步事件的投递状态，供管理接口查询。
     *
     * @param eventId 事件编号
     * @param requestId 关联的预占请求编号
     * @param destination 投递目标
     * @param status 当前投递状态
     * @param attempts 投递尝试次数
     * @param lastError 最近一次投递错误信息
     */
    public record EventView(String eventId, String requestId, String destination, String status,
                            int attempts, String lastError) {}

    /**
     * 统一接口错误响应。
     *
     * @param code 错误码
     * @param message 错误说明
     */
    public record ApiError(String code, String message) {}
}
