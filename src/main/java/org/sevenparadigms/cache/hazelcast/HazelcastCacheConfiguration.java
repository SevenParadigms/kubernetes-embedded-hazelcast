package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Objects;

@Configuration(proxyBeanMethods = false)
public class HazelcastCacheConfiguration {
    @Bean
    public HazelcastInstance hazelcastInstance(Environment env) {
        Config config = new Config();

        config.getJetConfig().setEnabled(true);

        config.setNetworkConfig(new NetworkConfig().setJoin(new JoinConfig()
                .setMulticastConfig(new MulticastConfig().setEnabled(true))));

        var timeout = Objects.isNull(env.getProperty("timeoutMinutes")) ? "5" : env.getProperty("timeoutMinutes");
        assert timeout != null;
        config.addMapConfig(new MapConfig("default")
                .setTimeToLiveSeconds(Integer.parseInt(timeout) * 60)
                .setEvictionConfig(new EvictionConfig()
                        .setEvictionPolicy(EvictionPolicy.LFU)
                        .setMaxSizePolicy(MaxSizePolicy.FREE_HEAP_SIZE)));

        if (Objects.equals(env.getProperty("kubernetes"), "true")) {
            var name = Objects.isNull(env.getProperty("serviceName")) ? "dev" : env.getProperty("serviceName");
            config.getNetworkConfig().getJoin().setKubernetesConfig(new KubernetesConfig()
                    .setEnabled(true)
                    .setProperty("namespace", "default")
                    .setProperty("service-name", name));
        }
        return Hazelcast.newHazelcastInstance(config);
    }

    @Bean
    public CacheManager hazelcastCacheManager(HazelcastInstance hazelcastInstance) {
        return new HazelcastCacheManager(hazelcastInstance);
    }
}
