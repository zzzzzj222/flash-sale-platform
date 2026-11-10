package com.flashsale.messaging;

import com.flashsale.api.Contracts.RequestMessage;
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
     * 监听请求队列，将消息体解析为申请消息并交由业务服务处理。
     * 解析或业务处理异常向监听容器传播，由容器决定后续处理。
     *
     * @param message RabbitMQ 接收到的原始消息
     */
    @RabbitListener(queues = "${sale.queue-name}")
    public void receive(Message message) {
        var request = json.readValue(message.getBody(), RequestMessage.class);
        service.process(request);
    }
}
