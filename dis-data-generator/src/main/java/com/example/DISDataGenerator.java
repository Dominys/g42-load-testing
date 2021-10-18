package com.example;

import com.example.config.DisConfig;
import com.example.model.PidDataRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import com.huaweicloud.dis.adapter.kafka.clients.producer.DISKafkaProducer;
import com.huaweicloud.dis.adapter.kafka.clients.producer.ProducerRecord;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jeasy.random.EasyRandom;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

@Slf4j
class DISDataGenerator {

    AtomicLong requestsCounter = new AtomicLong();

    private DISKafkaProducer<String, String> producer;
    private ObjectMapper objectMapper;


    public static void main(String[] args) throws IOException {
        new DISDataGenerator().run(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2]);
    }

    private void runRpsCounter(PrintWriter printWriter) {
        new Timer().scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long requestsCount = requestsCounter.getAndSet(0);
                log.info("Actual output is {} msg/s", requestsCount);
                printWriter.println(System.currentTimeMillis() + ";" + requestsCount);
                printWriter.flush();
            }
        }, 0, 1000);
    }

    private void run(int tasksCount, int messageRate, String streamName) throws IOException {
        producer = new DISKafkaProducer<>(DisConfig.getCommonConfig());
        objectMapper = new ObjectMapper();

        FileWriter fileWriter = new FileWriter("rps.txt");
        PrintWriter printWriter = new PrintWriter(fileWriter);
        runRpsCounter(printWriter);

        log.info("Starting {} treads to send data with {}msg/s rate to {}", tasksCount, messageRate, streamName);

        ExecutorService executorService = Executors.newFixedThreadPool(tasksCount);

        RateLimiter rateLimiter = RateLimiter.create(messageRate, 10, TimeUnit.SECONDS);
        EasyRandom easyRandom = new EasyRandom();
        CompletableFuture<?>[] futures = IntStream.range(0, tasksCount)
                .boxed()
                .map(f -> CompletableFuture.runAsync(() -> {
                    while(true) {
                        rateLimiter.acquire();
                        PidDataRequest pidDataRequest = easyRandom.nextObject(PidDataRequest.class);
                        producer.send(new ProducerRecord<>(streamName, toJson(pidDataRequest)), (recordMetadata, e) -> {
                            if (e != null) {
                                log.error("Failed to send message {}", pidDataRequest, e);
                            } else {
                                requestsCounter.incrementAndGet();
                                log.debug("Successfully send message {}", pidDataRequest);
                            }
                        });
                    }
                }, executorService))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
        printWriter.close();
    }

    @SneakyThrows
    private String toJson(Object o) {
        return objectMapper.writeValueAsString(o);
    }

}
