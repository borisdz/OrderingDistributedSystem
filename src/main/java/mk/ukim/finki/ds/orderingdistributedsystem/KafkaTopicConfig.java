package mk.ukim.finki.ds.orderingdistributedsystem;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "kafka.topics")
public class KafkaTopicConfig {
    private String orderPlacedUs;
    private String orderPlacedEu;
    private String availabilityResponse;

    public String orderPlacedTopic(String region) {
        return switch (region.toLowerCase()) {
            case "us" -> orderPlacedUs;
            case "eu" -> orderPlacedEu;
            default -> throw new IllegalArgumentException("Unsupported customer region: " + region);
        };
    }
}