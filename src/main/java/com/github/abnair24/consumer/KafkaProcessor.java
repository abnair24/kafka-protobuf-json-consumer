package com.github.abnair24.consumer;

import com.github.abnair24.protobuf.ProtobufToJson;
import com.github.abnair24.util.ProtoBufDecoder;
import com.github.abnair24.util.ProtoDetail;
import com.github.abnair24.util.TimeUtil;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaProcessor {

    private final String bootstrapServer;
    private final String groupId;
    private final List<String> topics;
    private final String protoPath;
    private final String fullMethodName;
    private final String key;
    private KafkaConsumer<byte[],byte[]> kafkaConsumer;
    private KafkaConsumerRunner kafkaConsumerRunner;
    private BlockingQueue<JsonObject> blockingQueue;
    private ProtoDetail protoDetail;
    private ProtoBufDecoder protobufDecoder;
    private Descriptors.Descriptor descriptor;
    private QueueConsumer queueConsumer;
    private AtomicBoolean closed;

    public KafkaProcessor(String bootstrapServer, String groupId, List<String> topics, String protoPath, String fullMethodName, String key) {
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
        this.topics = topics;
        this.protoPath = protoPath;
        this.fullMethodName = fullMethodName;
        this.blockingQueue = new ArrayBlockingQueue<>(1000);
        this.key = key;
        closed = new AtomicBoolean(false);
    }

    private void setConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        kafkaConsumer = new KafkaConsumer<>(properties);
    }

    private void decodeProtobuf() {
        protoDetail = new ProtoDetail(protoPath,fullMethodName);
        protobufDecoder = new ProtoBufDecoder(protoDetail);
    }

    private void getDescriptor() throws Exception {
        descriptor = protobufDecoder.invokeDescriptorBinary();
    }

    private void subscribeConsumer() {
        kafkaConsumer.subscribe(topics);
    }

    public JsonObject init() throws Exception {
        setConsumerProperties();
        subscribeConsumer();

        decodeProtobuf();
        getDescriptor();
        new Thread(new KafkaConsumerRunner(this)).start();
        queueConsumer = new QueueConsumer(blockingQueue,key);
        JsonObject jsonObject = queueConsumer.start(closed);
        shutdown();
        return jsonObject;
    }

    public void shutdown() {
        try {
            if (kafkaConsumerRunner != null) {
                kafkaConsumerRunner.shutdownConsumer();
            }
            if (kafkaConsumer != null) {
                    kafkaConsumer.close();
            }
        }catch (Exception ex) {
            System.out.println(ex.toString());
            log.error("Failed to shutdown cosumer : {}",ex.toString(),ex);
        }
    }

    static class KafkaConsumerRunner implements Runnable {

        private final KafkaProcessor kafkaProcessor;
        private ProtobufToJson protobufToJson;

        private long startTimestamp;

        public KafkaConsumerRunner(KafkaProcessor kafkaProcessor) {
            this.kafkaProcessor = kafkaProcessor;
        }

        @Override
        public void run() {
            try {
                startTimestamp = TimeUtil.getCurrentTimeInMilliseconds();
                protobufToJson = new ProtobufToJson(kafkaProcessor.descriptor);
                while(!kafkaProcessor.closed.get()) {
                    ConsumerRecords<byte[], byte[]> records = kafkaProcessor.kafkaConsumer.poll(Duration.ofMillis(1000));
                    System.out.println(records.count());
                    if(records.count()>0){
                        records.forEach(record ->processRecords(record));
                    }
                }
                log.info("closed status : {}",kafkaProcessor.closed.get());
            }catch(WakeupException ex) {
                if(!kafkaProcessor.closed.get()){
                    log.error("Exception on closing : {} ",ex.getMessage());
                    throw ex;
                }
            }finally {
                log.info("In finally");
                kafkaProcessor.kafkaConsumer.close();
            }
        }

        private void processRecords(ConsumerRecord<byte[],byte[]>record) {
            if (record.timestamp() > startTimestamp) {
                JsonObject jsonObject = protobufToJson.protobufToJsonObject(record.value());
                if (!putJsonObjectInQueue(jsonObject)) {
                    return;
                }
            }
        }

        private boolean putJsonObjectInQueue(JsonObject jsonObject) {
            try {
                kafkaProcessor.blockingQueue.put(jsonObject);
                return true;
            }catch (InterruptedException ex) {
                log.error("Failed to put record : {}",ex.toString(),ex);
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private void shutdownConsumer(){
            log.info("consumer shutdown");
            kafkaProcessor.closed.set(true);
            kafkaProcessor.kafkaConsumer.wakeup();
        }

    }
}
