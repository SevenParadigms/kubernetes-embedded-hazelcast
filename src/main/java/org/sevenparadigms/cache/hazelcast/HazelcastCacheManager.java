package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.BeansException;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import reactor.util.function.Tuples;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HazelcastCacheManager implements CacheManager, ApplicationContextAware {
    @Nullable private static ApplicationContext applicationContext = null;
    private final HazelcastInstance hazelcastInstance;
    private final Map<String, Cache> cacheMap = new ConcurrentHashMap<>();

    public HazelcastCacheManager(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    @NonNull
    public Collection<String> getCacheNames() {
        return Collections.unmodifiableSet(this.cacheMap.keySet());
    }

    @Override
    @Nullable
    public Cache getCache(@NonNull String name) {
        if (!cacheMap.containsKey(name)) {
            assert applicationContext != null;
            var expireAfterAccess = applicationContext.getEnvironment()
                    .getProperty("spring.cache." + name + ".expireAfterAccess", "-1");
            var expireAfterWrite = applicationContext.getEnvironment()
                    .getProperty("spring.cache." + name + ".expireAfterWrite", "-1");
            var maximumSize = applicationContext.getEnvironment()
                    .getProperty("spring.cache." + name + ".maximumSize", "-1");
            this.cacheMap.put(name, new GuidedCache(name, hazelcastInstance.getMap(name),
                    Tuples.of(expireAfterAccess, expireAfterWrite, maximumSize)));
        }
        return cacheMap.get(name);
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        HazelcastCacheManager.applicationContext = applicationContext;
    }
}