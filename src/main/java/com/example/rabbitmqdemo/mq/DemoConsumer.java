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
            ackMode = "MANUAL"
    )
    public void consume(
            DemoMessage message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {

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

        channel.basicAck(deliveryTag, false);
        System.out.println("acked deliveryTag = " + deliveryTag);

    }

}
