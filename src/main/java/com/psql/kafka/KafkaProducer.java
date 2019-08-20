package com.psql.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
            logger.info("Sending data to kafka = {} with topic {}", jsonData, topic);
            JSONObject dataObject = new JSONObject(jsonData);
            JSONArray dataArray = dataObject.getJSONArray("change");
            int dataLength = dataArray.length();
            for (int index = 0; index < dataLength; index++) {
                String operation = (String) dataArray.getJSONObject(index).get("kind"); //operation
                String table = (String) dataArray.getJSONObject(index).get("table");
                kafkaTemplate.send("audit_log1", jsonData);
            }
        } catch (Exception e) {
            logger.error("An error occurred! '{}'", e.getMessage());
        }
    }

}
