package com.combotto.controlplane;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.combotto.controlplane.api.DeviceFirmwareStateEvent;
import com.combotto.controlplane.setup.KafkaConsumerConfig;

@SpringJUnitConfig(KafkaConsumerConfig.class)
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=test-broker:19092")
class KafkaConsumerConfigTest {

  @Autowired
  ConcurrentKafkaListenerContainerFactory<String, String> stringKafkaListenerContainerFactory;

  @Autowired
  ConcurrentKafkaListenerContainerFactory<String, DeviceFirmwareStateEvent>
      deviceFirmwareStateKafkaListenerContainerFactory;

  @Test
  void stringConsumerFactoryUsesReplayFriendlyDefaults() {
    var consumerFactory =
        (DefaultKafkaConsumerFactory<?, ?>) stringKafkaListenerContainerFactory.getConsumerFactory();
    Map<String, Object> configs = consumerFactory.getConfigurationProperties();

    assertThat(configs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
        .isEqualTo("test-broker:19092");
    assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))
        .isEqualTo("earliest");
    assertThat(configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG))
        .isEqualTo(false);
  }

  @Test
  void deviceFirmwareStateConsumerFactoryUsesJsonDeserialization() {
    var consumerFactory =
        (DefaultKafkaConsumerFactory<?, ?>)
            deviceFirmwareStateKafkaListenerContainerFactory.getConsumerFactory();
    Map<String, Object> configs = consumerFactory.getConfigurationProperties();

    assertThat(configs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
        .isEqualTo("test-broker:19092");
    assertThat(configs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG))
        .isEqualTo(JacksonJsonDeserializer.class);
    assertThat(configs.get(JacksonJsonDeserializer.TRUSTED_PACKAGES))
        .isEqualTo("com.combotto.controlplane.*");
    assertThat(configs.get(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE))
        .isEqualTo(DeviceFirmwareStateEvent.class.getName());
  }
}
