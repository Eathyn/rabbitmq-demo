package com.example.rabbitmqdemo;

import com.example.rabbitmqdemo.mq.DemoProducer;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DemoController {

    private final DemoProducer producer;

    public DemoController(DemoProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/messages/{jobId}")
    public String send(@PathVariable long jobId) {
        producer.send(jobId);
        return "send jobId = " + jobId;
    }

    @PostMapping("/messages/unroutable/{jobId}")
    public String sendUnroutable(@PathVariable long jobId) {
        producer.sendWithWrongRoutingKey(jobId);
        return "send unroutable jobId = " + jobId;
    }

    @PostMapping("/messages/persistent/{jobId}")
    public String sendPersistent(@PathVariable long jobId) {
        producer.sendPersistent(jobId);
        return "send persistent jobId = " + jobId;
    }

    @PostMapping("/messages/transient/{jobId}")
    public String sendTransient(@PathVariable long jobId) {
        producer.sendTransient(jobId);
        return "send transient jobId = " + jobId;
    }

    @PostMapping("/messages/unknown/{jobId}")
    public String sendUnknown(@PathVariable long jobId) {
        producer.sendWithUnknownConfirm(jobId);
        return "send with unknown confirm jobId = " + jobId;
    }

}
