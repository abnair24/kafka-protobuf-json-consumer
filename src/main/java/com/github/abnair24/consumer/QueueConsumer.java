package com.github.abnair24.consumer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class QueueConsumer {

    private static final int THREAD_COUNT = 1;
    private final BlockingQueue<JsonObject> blockingQueue;
    private final String key;

    public QueueConsumer(BlockingQueue<JsonObject> blockingQueue, String key) {
        this.blockingQueue = blockingQueue;
        this.key = key;
    }

    public JsonObject start(AtomicBoolean closed) throws ExecutionException, InterruptedException {

        ExecutorService executorService  = Executors.newFixedThreadPool(2);
        Future<JsonObject> jsonObjectFuture = executorService.submit( () -> {
            try {
                while(!closed.get()) {
                    JsonObject jsonObject = blockingQueue.take();
                    for (Map.Entry<String, JsonElement> json : jsonObject.entrySet()) {

                        log.info("Waiting for :{}", key.toString());
                        if (json.getValue().getAsString().equals(key)) {
                            System.out.println("key found");
                            closed.set(true);

                        }
                        return jsonObject;
                    }
                }
            }catch (InterruptedException ex) {
                log.error("Exception");
                Thread.currentThread().interrupt();
            }
            return null;
        });

        return jsonObjectFuture.get();
    }
}
