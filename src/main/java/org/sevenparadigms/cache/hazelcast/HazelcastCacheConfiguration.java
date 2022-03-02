package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties
@Configuration(proxyBeanMethods = false)
public class HazelcastCacheConfiguration {
    @Bean
    public HazelcastInstance hazelcastInstance(HazelcastProperties properties) {
        Config config = new Config();

        config.getJetConfig().setEnabled(true);

        config.setNetworkConfig(new NetworkConfig().setJoin(new JoinConfig()
                .setTcpIpConfig(new TcpIpConfig().setEnabled(true))
                .setMulticastConfig(new MulticastConfig().setEnabled(true))));

        config.addMapConfig(new MapConfig("default")
                .setTimeToLiveSeconds(properties.getTimeoutMinutes() * 60)
                .setEvictionConfig(new EvictionConfig().setEvictionPolicy(EvictionPolicy.LFU)));

        if (properties.getKubernetes()) {
            config.getNetworkConfig().getJoin().setKubernetesConfig(new KubernetesConfig()
                    .setEnabled(true)
                    .setProperty("namespace", "default")
                    .setProperty("service-name", properties.getServiceName()));
        }
        return Hazelcast.newHazelcastInstance(config);
    }

    @Bean
    public CacheManager hazelcastCacheManager(HazelcastInstance hazelcastInstance) {
        return new HazelcastCacheManager(hazelcastInstance);
    }
}
