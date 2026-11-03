package com.example.rabbitmqdemo.mq;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class DemoConsumer {

    private boolean nackOnce = true;

    @RabbitListener(
            queues = RabbitConfig.QUEUE,
            ackMode = "MANUAL",
            autoStartup = "true" // 禁止 Consumer 自动启动
    )
    public void consume(
            DemoMessage message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag,
            @Header(name = AmqpHeaders.REDELIVERED, required = false) Boolean redelivered
    ) throws IOException {

        System.out.println("redelivered = " + redelivered);
        System.out.println("received jobId = " + message.jobId());
        System.out.println("delivery tag = " + deliveryTag);

        if (message.jobId() == 998 && nackOnce) {
            nackOnce = false;
            channel.basicNack(deliveryTag, false, true);
            System.out.println("nacked and requeued delivery tag = " + deliveryTag);
            return;
        }

        if (message.jobId() == 999) {
            channel.basicReject(deliveryTag, false);
            System.out.println("rejected delivery tag = " + deliveryTag);
            return;
        }

        if (message.jobId() == 804 && !Boolean.TRUE.equals(redelivered)) {

            System.out.println("jobId=804 waiting before basicAck");

            try {
                Thread.sleep(300_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("jobId=804 interrupted before basicAck");
                return;
            }
        }

        channel.basicAck(deliveryTag, false);
        System.out.println("acked deliveryTag = " + deliveryTag);

    }

}
