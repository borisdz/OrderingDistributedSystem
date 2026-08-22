package mk.ukim.finki.ds.orderingdistributedsystem.config;

import mk.ukim.finki.ds.contracts.events.AvailabilityCheckedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.lang.NonNull;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    @NonNull
    public ConsumerFactory<String, AvailabilityCheckedEvent> availabilityConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service-aggregator");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<AvailabilityCheckedEvent> deserializer = new JsonDeserializer<>(AvailabilityCheckedEvent.class);
        deserializer.setRemoveTypeHeaders(false);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(true);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    @NonNull
    public ConcurrentKafkaListenerContainerFactory<String, AvailabilityCheckedEvent> availabilityKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, AvailabilityCheckedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        ConsumerFactory<? super String, ? super AvailabilityCheckedEvent> consumerFactory = availabilityConsumerFactory();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }
}
