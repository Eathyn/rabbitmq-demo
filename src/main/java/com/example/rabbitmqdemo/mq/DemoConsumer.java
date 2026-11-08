package com.example.rabbitmqdemo.mq;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class DemoConsumer {

    private boolean nackOnce = true;
    private boolean requeueOnceFor101 = true;
    private JdbcTemplate jdbcTemplate;

    public DemoConsumer(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

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

        if (message.jobId() == 200) {

            int affectedRows = jdbcTemplate.update("""
                    UPDATE demo_job
                    SET status = 'RUNNING',
                        attempt_count = attempt_count + 1
                    WHERE id = ?
                      AND status = 'PENDING'
                      AND attempt_count < 3
                    """,
                    message.jobId()
            );

            System.out.println("business retry demo claim affectedRows = " + affectedRows);

            if (affectedRows == 1) {
                RetryDemoJob job = findRetryDemoJob(message.jobId());

                if (job.attemptCount() == 1) {
                    // 教学控制手段，非生产实现：
                    // 第一次 attempt 固定模拟业务失败。
                    jdbcTemplate.update("""
                    UPDATE demo_job
                    SET status = 'FAILED'
                    WHERE id = ?
                      AND status = 'RUNNING'
                    """,
                            message.jobId()
                    );

                    System.out.println("simulated business failure on attempt #1");
                } else {
                    jdbcTemplate.update("""
                    UPDATE demo_job
                    SET execute_count = execute_count + 1
                    WHERE id = ?
                    """,
                            message.jobId()
                    );

                    System.out.println("business execution on attempt #" + job.attemptCount());
                }
            }

            printRetryDemoJob(message.jobId());

            channel.basicAck(deliveryTag, false);
            System.out.println("acked deliveryTag = " + deliveryTag);
            return;
        }

        if (message.jobId() == 100 || message.jobId() == 101) {

            int affectedRows = jdbcTemplate.update("""
                            UPDATE demo_job
                            SET status = 'RUNNING'
                            WHERE id = ?
                                AND status = 'PENDING'
                            """,
                    message.jobId()
            );

            System.out.println("claim affectedRows = " + affectedRows);

            if (message.jobId() == 101
                && affectedRows == 1
                && requeueOnceFor101) {

                requeueOnceFor101 = false;

                System.out.println("claim succeeded but skip execute for teaching crash window");

                printJob(message.jobId());

                channel.basicNack(deliveryTag, false, true);
                System.out.println("nacked and requeued deliveryTag = " + deliveryTag);

                return;

            }

            if (affectedRows == 1) {
                System.out.println("claim succeeded, execute jobId = " + message.jobId());

                jdbcTemplate.update("""
                                UPDATE demo_job
                                SET execute_count = execute_count + 1
                                WHERE id = ?
                                """,
                        message.jobId()
                );
            } else {
                System.out.println("claim failed, skip execute jobId = " + message.jobId());
            }

            DemoJob job = jdbcTemplate.queryForObject("""
                            SELECT id, status, execute_count
                            FROM demo_job
                            WHERE id = ?
                            """,
                    (rs, rowNum) -> new DemoJob(
                            rs.getLong("id"),
                            rs.getString("status"),
                            rs.getInt("execute_count")
                    ),
                    message.jobId()
            );

            System.out.println("status = " + job.status());
            System.out.println("executeCount = " + job.executeCount());

            channel.basicAck(deliveryTag, false);
            System.out.println("acked deliveryTag = " + deliveryTag);

            return;
        }

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

    private record DemoJob(
            long id,
            String status,
            int executeCount
    ) {
    }

    private void printJob(long jobId) {

        DemoJob job = jdbcTemplate.queryForObject("""
                    SELECT id, status, execute_count
                    FROM demo_job
                    WHERE id = ?
                    """,
                (rs, rowNum) -> new DemoJob(
                        rs.getLong("id"),
                        rs.getString("status"),
                        rs.getInt("execute_count")
                ),
                jobId
        );

        System.out.println("status = " + job.status());
        System.out.println("executeCount = " + job.executeCount());
    }

    private RetryDemoJob findRetryDemoJob(long jobId) {
        return jdbcTemplate.queryForObject("""
            SELECT id, status, execute_count, attempt_count
            FROM demo_job
            WHERE id = ?
            """,
                (rs, rowNum) -> new RetryDemoJob(
                        rs.getLong("id"),
                        rs.getString("status"),
                        rs.getInt("execute_count"),
                        rs.getInt("attempt_count")
                ),
                jobId
        );
    }

    private void printRetryDemoJob(long jobId) {
        RetryDemoJob job = findRetryDemoJob(jobId);

        System.out.println("status = " + job.status());
        System.out.println("executeCount = " + job.executeCount());
        System.out.println("attemptCount = " + job.attemptCount());
    }

    private record RetryDemoJob(
            long id,
            String status,
            int executeCount,
            int attemptCount
    ) {
    }
}
