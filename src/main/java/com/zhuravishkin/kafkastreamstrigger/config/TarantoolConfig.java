package com.zhuravishkin.kafkastreamstrigger.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientConfig;
import org.tarantool.TarantoolClientImpl;

@Configuration
public class TarantoolConfig {
    @Bean
    public TarantoolClientConfig tarantoolClientConfig() {
        final TarantoolClientConfig config = new TarantoolClientConfig();
        config.username = "sa";
        config.password = "tnt";
        config.useNewCall = true;
        return config;
    }

    @Bean
    public TarantoolClient tarantoolClient() {
        return new TarantoolClientImpl("192.168.0.13:3301", tarantoolClientConfig());
    }
}
