package com.combotto.controlplane.setup;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import com.combotto.controlplane.api.DeviceFirmwareStateEvent;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> stringKafkaListenerContainerFactory(
      @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    ConcurrentKafkaListenerContainerFactory<String, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(cfg));
    return factory;
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, DeviceFirmwareStateEvent>
      deviceFirmwareStateKafkaListenerContainerFactory(
          @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
    cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cfg.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.combotto.controlplane.*");
    cfg.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, DeviceFirmwareStateEvent.class.getName());

    ConcurrentKafkaListenerContainerFactory<String, DeviceFirmwareStateEvent> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(cfg));
    return factory;
  }
}
