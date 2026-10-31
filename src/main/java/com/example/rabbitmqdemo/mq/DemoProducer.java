package com.example.rabbitmqdemo.mq;

import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class DemoProducer {

    private final RabbitTemplate rabbitTemplate;

    public DemoProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void send(long jobId) {

        DemoMessage message = new DemoMessage(jobId);
        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());

        rabbitTemplate.convertAndSend(
                RabbitConfig.EXCHANGE,
                RabbitConfig.ROUTING_KEY,
                message,
                correlationData
        );

        try {
            // 5 秒内拿不到结果就会抛出异常
            CorrelationData.Confirm confirm = correlationData
                    .getFuture()
                    .get(5, TimeUnit.SECONDS);
            System.out.println("confirm ack = " + confirm.isAck());
            System.out.println("confirm reason = " + confirm.getReason());
        } catch (Exception e) {
            System.out.println("等待 Publisher Confirm 失败或超时");
            e.printStackTrace();
        }

    }

    public void sendWithWrongRoutingKey(long jobId) {

        DemoMessage message = new DemoMessage(jobId);
        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());

        rabbitTemplate.convertAndSend(
                RabbitConfig.EXCHANGE,
                RabbitConfig.WRONG_ROUTING_KEY,
                message,
                correlationData
        );

        try {
            // 5 秒内拿不到结果就会抛出异常
            CorrelationData.Confirm confirm = correlationData
                    .getFuture()
                    .get(5, TimeUnit.SECONDS);
            System.out.println("confirm ack = " + confirm.isAck());
            System.out.println("confirm reason = " + confirm.getReason());
            ReturnedMessage returned = correlationData.getReturned();

            if (returned == null) {
                System.out.println("returned message = null");
            } else {
                System.out.println("returned message != null");
                System.out.println("returned exchange = " + returned.getExchange());
                System.out.println("returned routingKey = " + returned.getRoutingKey());
                System.out.println("returned replyText = " + returned.getReplyText());
            }

        } catch (Exception e) {
            System.out.println("等待 Publisher Confirm 失败或超时");
            e.printStackTrace();
        }

    }

}
