package com.github.abnair24.kafkaConsumer;


import com.google.gson.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

@Getter
@Setter
@AllArgsConstructor
public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private String bootstrapServer;
    private String groupId;
    private List<String> topics;
    private String protoPath;
    private String fullMethodName;

    public List<JsonObject> init(int totalMessages) {

        Properties properties = consumerProperties(bootstrapServer,groupId);
        KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer(properties);
        consumer.subscribe(topics);
        return getMessagesFromTopic(consumer,totalMessages);
    }

    private List<JsonObject> getMessagesFromTopic(KafkaConsumer<byte[],byte[]>consumer, int totalMessages) {
        int messagesCount = 0;
        boolean isRead = true;
        List<JsonObject> jsonObjectList = new ArrayList<>();

        while(isRead) {
            ConsumerRecords<byte[],byte[]> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<byte[],byte[]> record : records) {
                messagesCount += 1;
                JsonObject jsonObject = null;
                try {
                    jsonObject = ProtobufToJson.protobufToJson(protoPath,fullMethodName,record.value());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                jsonObjectList.add(jsonObject);

                if(messagesCount >= totalMessages ){
                    isRead = false;
                    break;
                }
            }
        }
        return jsonObjectList;
    }

    private Properties consumerProperties(String server,String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"500");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");

        return properties;
    }


}
