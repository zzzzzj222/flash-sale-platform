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

    public RequestConsumer(SaleService service, ObjectMapper json) {
        this.service = service;
        this.json = json;
    }

    @RabbitListener(queues = "${sale.queue-name}")
    public void receive(Message message) {
        var request = json.readValue(message.getBody(), RequestMessage.class);
        service.process(request.tenantId(), request.requestId());
    }
}
