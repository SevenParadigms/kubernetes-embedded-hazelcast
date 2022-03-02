package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.config.Config;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@ConfigurationProperties(prefix = "hazelcast")
public class HazelcastProperties {
    private Boolean kubernetes = Boolean.FALSE;
    private Integer timeoutMinutes = 5;
    private String serviceName = Config.DEFAULT_CLUSTER_NAME;
}
