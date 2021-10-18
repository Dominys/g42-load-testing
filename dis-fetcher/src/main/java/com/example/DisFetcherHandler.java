package com.example;

import com.example.model.PidDataRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huawei.services.runtime.Context;
import com.huawei.services.runtime.entity.dis.DISRecord;
import com.huawei.services.runtime.entity.dis.DISTriggerEvent;
import com.huawei.services.runtime.entity.timer.TimerTriggerEvent;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DisFetcherHandler {

    private ObjectMapper objectMapper;

    public void init(Context context) throws InterruptedException {
        log.info("Start init");
        objectMapper = new ObjectMapper();
        if (Boolean.parseBoolean(System.getenv("FAIL_INIT"))) {
            Thread.sleep(2000L);
            throw new FetcherException("Init failed by config");
        }
    }

    public String handleRequest(DISTriggerEvent input, Context context) {
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        ThreadContext.put("G42RequestId", context.getRequestID());
        List<PidDataRequest> pidDataRequests = Arrays.stream(input.getMessage().getRecords())
                .map(this::getGetRawData)
                .map(f -> readValue(f, PidDataRequest.class))
                .map(this::process)
                .collect(Collectors.toList());

        log.info("Received {} records from DIS", pidDataRequests.size());
        return "Success event";

    }

    @SneakyThrows
    private String getGetRawData(DISRecord disRecord) {
        return disRecord.getRawData();
    }

    @SneakyThrows
    private PidDataRequest process(PidDataRequest pidDataRequest) {
        //Simulate data processing
        Thread.sleep(100L);
        log.info("Processed record for devId={}", pidDataRequest.getDevId());
        return pidDataRequest;
    }

    private <T> T readValue(String s, Class<T> tClass) {
        try {
            log.info("Record JSON: {}", s);
            if (Boolean.parseBoolean(System.getenv("FAIL_READ"))) {
                throw new FetcherException("readValue failed by config");
            }
            return objectMapper.readValue(s, tClass);
        } catch (JsonProcessingException e) {
            throw new FetcherException("Jackson error", e);
        }
    }
}
