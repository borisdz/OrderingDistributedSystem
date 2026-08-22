package mk.ukim.finki.ds.orderingdistributedsystem;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "aggregation")
public class AggregationConfig {
    private Map<String, Integer> expectedResponses = new HashMap<>();
    private long timeoutSeconds = 30;
    private long cleanupIntervalSeconds = 10;
}