package com.flashsale.messaging;

import com.flashsale.api.Contracts.RequestMessage;
import java.util.ArrayList;
import com.flashsale.core.SaleService;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@ConditionalOnProperty(name = "sale.workers-enabled", havingValue = "true", matchIfMissing = true)
public class RequestConsumer {
    private final SaleService service;
    private final ObjectMapper json;

    /**
     * 初始化申请消息消费所需的业务处理和 JSON 反序列化组件。
     *
     * @param service 秒杀申请处理服务
     * @param json JSON 序列化与反序列化组件
     */
    public RequestConsumer(SaleService service, ObjectMapper json) {
        this.service = service;
        this.json = json;
    }

    /**
     * 监听请求队列，兼容旧单条消息和生产端小批次，事务提交后由容器确认消息。
     * 批次内的数据或不变式错误改为逐条重试，避免一条坏申请拖整批进死信。
     * 解析或数据库异常向监听容器传播，由容器决定后续处理。
     *
     * @param message RabbitMQ 接收到的原始消息
     */
    @RabbitListener(queues = "${sale.queue-name}")
    public void receive(Message message) {
        var payload = json.readTree(message.getBody());
        if (!payload.isArray()) {
            service.process(json.treeToValue(payload, RequestMessage.class));
            return;
        }
        if (payload.size() > SaleService.MAX_BATCH_SIZE) { throw new IllegalArgumentException("Reservation batch too large"); }
        var requests = new ArrayList<RequestMessage>();
        for (var item : payload) { requests.add(json.treeToValue(item, RequestMessage.class)); }
        try {
            service.processBatch(requests);
        } catch (IllegalArgumentException | IllegalStateException batchError) {
            // 逐条重试是幂等的；单条失败仍抛给容器，其余申请已经提交。
            RuntimeException failure = null;
            for (var request : requests) {
                try { service.process(request); }
                catch (RuntimeException error) { if (failure == null) { failure = error; } }
            }
            if (failure != null) { throw failure; }
        }
    }
}
