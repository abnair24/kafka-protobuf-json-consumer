package com.github.abnair24.consumer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class JsonQueueConsumer<T> {

    private static final int THREAD_COUNT = 1;
    private final BlockingQueue<JsonObject> blockingQueue;
    private final T key;

    public JsonQueueConsumer(BlockingQueue<JsonObject> blockingQueue, T key) {
        this.blockingQueue = blockingQueue;
        this.key = key;
    }

    public JsonObject start(AtomicBoolean closed) {

        ExecutorService executorService  = Executors.newFixedThreadPool(THREAD_COUNT);

        JsonObject json = null;

        Callable<JsonObject> task = () -> {
            while (!closed.get()) {
                JsonObject jsonObject = blockingQueue.take();
                for (Map.Entry<String, JsonElement> obj : jsonObject.entrySet()) {

                    log.debug("Waiting for :{}", key.toString());

                    if (obj.getValue().getAsString().equals(key)) {
                        closed.set(true);

                        log.debug("Key match found. Setting atomic boolean");
                    }
                    return jsonObject;
                }
            }
            return null;
        };

        Future<JsonObject> jsonObjectFuture = executorService.submit(task);

        try {
            json = jsonObjectFuture.get();
        } catch (ExecutionException | InterruptedException ex) {
            log.error("Exception on retrieving jsonObject : {}",ex.getMessage());
            ex.printStackTrace();
        }

        log.debug("Shutting down executor service for blockingQueue");
        executorService.shutdown();

        return json;
    }
}
