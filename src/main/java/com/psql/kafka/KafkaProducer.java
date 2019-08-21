package com.psql.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    private NewTopic topic = new NewTopic("audit_log1", 3, (short)1);;
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void postData( String jsonData){
        try {
            logger.info("Sending data to kafka = {} with topic {}", jsonData, topic.toString());
            kafkaTemplate.send("audit_log1", jsonData);
        } catch (Exception e) {
            logger.error("An error occurred! '{}'", e.getMessage());
        }
    }

}
