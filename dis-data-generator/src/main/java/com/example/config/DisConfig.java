package com.example.config;

import com.huaweicloud.dis.DISConfig;
import com.huaweicloud.dis.adapter.kafka.clients.producer.ProducerConfig;
import com.huaweicloud.dis.adapter.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DisConfig {

    public static Properties getCommonConfig() {
        Properties props = new Properties();
        props.setProperty(DISConfig.PROPERTY_AK, "*");
        props.setProperty(DISConfig.PROPERTY_SK, "*");
        props.setProperty(DISConfig.PROPERTY_PROJECT_ID, "0c8cbfe6ad005d1c2f57c0131a5c5da3");
        props.setProperty(DISConfig.PROPERTY_REGION_ID, "ae-ad-1");
        props.setProperty(DISConfig.PROPERTY_ENDPOINT, "https://dis.ae-ad-1.g42cloud.com");
        props.setProperty(DISConfig.PROPERTY_PRODUCER_BATCH_SIZE, "10485760");
        props.setProperty(DISConfig.PROPERTY_PRODUCER_BUFFER_MEMORY, "536870912");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

}
