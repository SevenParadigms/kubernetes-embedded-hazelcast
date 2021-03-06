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
        System.setProperty("hazelcast.jcache.provider.type", "server");
        var port = Objects.isNull(env.getProperty("hazelcast.port"))
                ? 5702 : Integer.parseInt(Objects.requireNonNull(env.getProperty("hazelcast.port")));
        config.getJetConfig().setEnabled(true);
        config.getNetworkConfig()
                .setPort(port)
                .setJoin(new JoinConfig().setMulticastConfig(new MulticastConfig().setEnabled(false)));
        if (Objects.equals(env.getProperty("hazelcast.kubernetes"), "true")) {
            var namespace = Objects.isNull(env.getProperty("hazelcast.namespace"))
                    ? "default" : env.getProperty("hazelcast.namespace");
            var applicationName = Objects.isNull(env.getProperty("spring.application.name"))
                    ? "develop" : env.getProperty("spring.application.name");
            var serviceName = Objects.isNull(env.getProperty("hazelcast.serviceName")) ?
                    applicationName : env.getProperty("hazelcast.serviceName");
            config.getNetworkConfig().getJoin().setKubernetesConfig(new KubernetesConfig()
                    .setEnabled(true)
                    .setProperty("namespace", namespace)
                    .setProperty("service-name", serviceName));
        }
        return Hazelcast.newHazelcastInstance(config);
    }

    @Bean
    public CacheManager hazelcastCacheManager(HazelcastInstance hazelcastInstance) {
        return new HazelcastGuidedCacheManager(hazelcastInstance);
    }
}
