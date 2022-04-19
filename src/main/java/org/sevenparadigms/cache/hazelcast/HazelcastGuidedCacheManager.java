package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.beans.BeansException;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.r2dbc.support.Beans;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class HazelcastGuidedCacheManager implements CacheManager, ApplicationContextAware {
    private final Map<String, Cache> cacheMap = new ConcurrentHashMap<>();
    private javax.cache.CacheManager cacheManager;
    private final HazelcastInstance hazelcastInstance;

    public HazelcastGuidedCacheManager(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        Properties properties = new Properties();
        properties.put(HazelcastCachingProvider.HAZELCAST_INSTANCE_ITSELF, hazelcastInstance);
        this.cacheManager = Caching.getCachingProvider().getCacheManager(null, null, properties);
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
            var expireAfterAccess = Beans.getProperty("spring.cache." + name + ".expireAfterAccess", Integer.class, -1);
            var expireAfterWrite = Beans.getProperty("spring.cache." + name + ".expireAfterWrite", Integer.class, -1);
            var maximumSize = Beans.getProperty("spring.cache." + name + ".maximumSize", Integer.class, -1);
            MutableConfiguration<Object, Object> config = new MutableConfiguration<>().setTypes(Object.class, Object.class);
            if (expireAfterAccess < 0 && expireAfterWrite < 0) {
                expireAfterWrite = Beans.getProperty("hazelcast.timeoutMinutes", Integer.class, 30) * 60 * 1000;
            }
            if (maximumSize < 0) {
                maximumSize = Beans.getProperty("hazelcast.maxSize", Integer.class, 1000);
            }
            javax.cache.Cache<Object, Object> cache = cacheManager.createCache(name, config);
            this.cacheMap.put(name, new GuidedCache(name, cache.unwrap(ICache.class), hazelcastInstance.getMap(name),
                    expireAfterAccess, expireAfterWrite, maximumSize));
        }
        return cacheMap.get(name);
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        Beans.setAndGetContext(applicationContext);
    }
}