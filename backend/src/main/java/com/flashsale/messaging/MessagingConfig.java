package com.flashsale.messaging;

import com.flashsale.SaleProperties;
import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.amqp.autoconfigure.ConnectionFactoryCustomizer;

@Configuration
public class MessagingConfig {
    @Bean
    ConnectionFactoryCustomizer boundedBrokerCalls() {
        return factory -> {
            factory.setHandshakeTimeout(3000);
            factory.setChannelRpcTimeout(3000);
            factory.setShutdownTimeout(1000);
        };
    }

    @Bean
    Declarables queues(SaleProperties properties) {
        String name = properties.queueName();
        var deadExchange = new DirectExchange(name + ".dead.exchange", true, false);
        var dead = QueueBuilder.durable(name + ".dead").quorum().build();
        var requests = QueueBuilder.durable(name).quorum()
                .withArgument("x-max-length", properties.queueLimit())
                .withArgument("x-overflow", "reject-publish")
                .deadLetterExchange(deadExchange.getName()).deadLetterRoutingKey("failed").build();
        return new Declarables(requests, dead, deadExchange,
                BindingBuilder.bind(dead).to(deadExchange).with("failed"));
    }
}
