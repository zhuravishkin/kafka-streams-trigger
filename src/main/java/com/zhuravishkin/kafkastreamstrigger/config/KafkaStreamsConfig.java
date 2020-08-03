package com.zhuravishkin.kafkastreamstrigger.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhuravishkin.kafkastreamstrigger.model.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.tarantool.Iterator;
import org.tarantool.TarantoolClient;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {
    TarantoolClient tarantoolClient;

    public KafkaStreamsConfig(TarantoolClient tarantoolClient) {
        this.tarantoolClient = tarantoolClient;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public Serde<User> userSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(User.class));
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, String> stream = kStreamBuilder
                .stream("src-topic", Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, User> userStream =
                stream.mapValues(this::getUserFromString);
        userStream.to("out-topic", Produced.with(Serdes.String(), userSerde()));
        return stream;
    }

    User getUserFromString(String userString) {
        User user = null;
        try {
            user = objectMapper().readValue(userString, User.class);
            tarantoolClient.asyncOps()
                    .insert("space", Arrays.asList(
                            user.getProfileId(),
                            user.getBucketId(),
                            user.getFirstName(),
                            user.getSurName(),
                            user.getEventTime()
                            )
                    );
            tarantoolClient.asyncOps()
                    .update(
                            "space",
                            Collections.singletonList(user.getProfileId()),
                            Arrays.asList("=", 2, user.getFirstName()),
                            Arrays.asList("=", 3, user.getSurName()),
                            Arrays.asList("=", 4, user.getEventTime())
                    );
            tarantoolClient.asyncOps()
                    .upsert(
                            "space",
                            Collections.singletonList(user.getProfileId()),
                            Arrays.asList(user.getFirstName(), user.getSurName(), user.getEventTime()),
                            Arrays.asList("=", 2, user.getFirstName()),
                            Arrays.asList("=", 3, user.getSurName()),
                            Arrays.asList("=", 4, user.getEventTime())
                    );
            Future<List<?>> listFuture = tarantoolClient.asyncOps().select("space", "bucket_id",
                    Collections.singletonList(1990), 0, 1, Iterator.EQ);
            List<String> list = listFuture.get().stream().map(String::valueOf)
                    .map(tuple -> List.of(tuple.substring(1, tuple.length() - 1).split(", ")))
                    .findFirst().orElse(Collections.emptyList());
            long time = Long.parseLong(list.get(4));
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
        return user;
    }
}
