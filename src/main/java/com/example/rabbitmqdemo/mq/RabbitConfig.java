package com.example.rabbitmqdemo.mq;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    public static final String EXCHANGE = "demo.exchange";
    public static final String QUEUE = "demo.queue";
    public static final String ROUTING_KEY = "demo.created";
    public static final String WRONG_ROUTING_KEY = "demo.wrong";
    public static final String DLX = "demo.dlx";
    public static final String DLQ = "demo.dlq";

    @Bean
    DirectExchange demoExchange() {
        return new DirectExchange(EXCHANGE, true,false);
    }

    @Bean
    Queue demoQueue() {
        return QueueBuilder.durable(QUEUE)
                .deadLetterExchange(DLX)
                .build();
    }

    @Bean
    DirectExchange demoDlx() {
        return new DirectExchange(DLX, true, false);
    }

    @Bean
    Queue demoDlq() {
        return QueueBuilder.durable(DLQ).build();
    }

    @Bean
    Binding demoBinding(
            @Qualifier("demoQueue") Queue demoQueue,
            @Qualifier("demoExchange") DirectExchange demoExchange
    ) {
        return BindingBuilder
                .bind(demoQueue)
                .to(demoExchange)
                .with(ROUTING_KEY);
    }

    @Bean
    Binding demoDlqBinding(
            @Qualifier("demoDlq") Queue demoDlq,
            @Qualifier("demoDlx") DirectExchange demoDlx
    ) {
        return BindingBuilder
                .bind(demoDlq)
                .to(demoDlx)
                .with(ROUTING_KEY);
    }

    @Bean
    MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

}
