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

}
