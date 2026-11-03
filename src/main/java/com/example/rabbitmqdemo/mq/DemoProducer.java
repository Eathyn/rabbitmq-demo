package com.example.rabbitmqdemo.mq;

import org.springframework.amqp.core.MessageDeliveryMode;
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

    public void sendPersistent(long jobId) {
        sendWithDeliveryMode(jobId, MessageDeliveryMode.PERSISTENT);
    }

    public void sendTransient(long jobId) {
        sendWithDeliveryMode(jobId, MessageDeliveryMode.NON_PERSISTENT);
    }

    private void sendWithDeliveryMode(
            long jobId,
            MessageDeliveryMode deliveryMode
    ) {

        DemoMessage message = new DemoMessage(jobId);
        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());

        rabbitTemplate.convertAndSend(
                RabbitConfig.EXCHANGE,
                RabbitConfig.ROUTING_KEY,
                message,
                amqpMessage -> {
                    amqpMessage.getMessageProperties()
                            .setDeliveryMode(deliveryMode);
                    return amqpMessage;
                },
                correlationData
        );

        try {
            CorrelationData.Confirm confirm = correlationData
                    .getFuture()
                    .get(5, TimeUnit.SECONDS);

            System.out.println("jobId = " + jobId);
            System.out.println("delivery mode = " + deliveryMode);
            System.out.println("confirm ack = " + confirm.isAck());
        } catch (Exception e) {
            System.out.println("等待 Publisher Confirm 失败或超时");
            e.printStackTrace();
        }
    }

    public void sendWithUnknownConfirm(long jobId) {

        DemoMessage message = new DemoMessage(jobId);
        CorrelationData correlationData =
                new CorrelationData(UUID.randomUUID().toString());

        rabbitTemplate.convertAndSend(
                RabbitConfig.EXCHANGE,
                RabbitConfig.ROUTING_KEY,
                message,
                amqpMessage -> {
                    amqpMessage.getMessageProperties()
                            .setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return amqpMessage;
                },
                correlationData
        );

        // 教学控制手段：
        // 假装应用在这里等待 Confirm 超时，
        // 故意不读取 correlationData 的真实最终结果。
        System.out.println("jobId = " + jobId);
        System.out.println("delivery mode = PERSISTENT");
        System.out.println("confirm result = UNKNOWN (simulated timeout)");
    }

}
