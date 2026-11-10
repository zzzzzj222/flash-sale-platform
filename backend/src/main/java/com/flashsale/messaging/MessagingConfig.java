package com.flashsale.messaging;

import com.flashsale.SaleProperties;
import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.amqp.autoconfigure.ConnectionFactoryCustomizer;

@Configuration
public class MessagingConfig {
    /**
     * 限制 RabbitMQ 握手、通道 RPC 和连接关闭的等待时间，避免调用长期阻塞。
     *
     * @return 应用连接超时设置的定制器
     */
    @Bean
    ConnectionFactoryCustomizer boundedBrokerCalls() {
        return factory -> {
            factory.setHandshakeTimeout(3000);
            factory.setChannelRpcTimeout(3000);
            factory.setShutdownTimeout(1000);
        };
    }

    /**
     * 声明持久化的 quorum 请求队列、死信队列及其交换机和绑定。
     * 请求队列达到配置上限时拒绝发布，被死信处理的消息通过 failed 路由进入死信队列。
     *
     * @param properties 请求队列名称和容量配置
     * @return 交由 Spring AMQP 声明的队列、交换机和绑定集合
     */
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
