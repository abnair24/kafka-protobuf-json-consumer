package com.github.abnair24.kafkaConsumer;


import com.github.abnair24.util.ProtoBufDecoder;
import com.github.abnair24.util.ProtoDetail;
import com.github.abnair24.util.ProtoUtility;
import com.github.abnair24.util.TimeUtil;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Getter
@Setter
@Slf4j
public class ProtobufConsumer {

    private final String bootstrapServer;
    private final String groupId;
    private final List<String> topics;
    private final String protoPath;
    private final String fullMethodName;
    private final ProtoDetail protoDetail;

    private ProtoBufDecoder protoBufDecoder;
    private Descriptors.Descriptor descriptor;

    public ProtobufConsumer(String bootstrapServer, String groupId, List<String> topics, String protoPath, String fullMethodName) {
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
        this.topics = topics;
        this.protoPath = protoPath;
        this.fullMethodName = fullMethodName;
        protoDetail = new ProtoDetail(protoPath,fullMethodName);
    }

    public List<JsonObject> init(int totalMessages) throws Exception {

        Properties properties = setConsumerProperties(bootstrapServer,groupId);
        KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer(properties);
        consumer.subscribe(topics);

        protoBufDecoder = new ProtoBufDecoder(protoDetail);
        descriptor = protoBufDecoder.invokeDescriptorBinary();

        return getMessagesFromTopic(consumer, totalMessages);
    }

    private List<JsonObject> getMessagesFromTopic(KafkaConsumer<byte[],byte[]>consumer,
                                                  int totalMessages) {
        int messagesCount = 0;
        boolean isRead = true;
        long startTimeStamp = TimeUtil.getCurrentTimeInMilliseconds();

        List<JsonObject> jsonObjectList = new ArrayList<>();
        ProtobufToJson protobufToJson = new ProtobufToJson(descriptor);

        while(isRead) {
            ConsumerRecords<byte[],byte[]> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<byte[],byte[]> record : records) {

                if(record.timestamp() > startTimeStamp) {
                    messagesCount += 1;
                    JsonObject jsonObject = null;
                    try {
                        jsonObject = protobufToJson.protobufToJsonObject(record.value());
                        jsonObjectList.add(jsonObject);

                        if (messagesCount >= totalMessages) {
                            isRead = false;
                            break;
                        }
                    } catch (Exception e) {
                        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return jsonObjectList;
    }

    private Properties setConsumerProperties(String server, String groupId) {
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
