package com.psql.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;



    public void postData(String topic, String jsonData){
        try {
            logger.info("Sending data to kafka = '{}' with topic '{}'", jsonData, topic);
            kafkaTemplate.send(topic,  jsonData);
        } catch (Exception e) {
            logger.error("An error occurred! '{}'", e.getMessage());
        }
    }

}
