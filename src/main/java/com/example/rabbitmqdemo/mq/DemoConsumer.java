package com.example.rabbitmqdemo.mq;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class DemoConsumer {

    @RabbitListener(queues = RabbitConfig.QUEUE)
    public void consume(DemoMessage message) {
        System.out.println("received jobId = " + message.jobId());
    }

}
