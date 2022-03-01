package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.lang.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HazelcastCacheManager implements CacheManager {
    private final HazelcastInstance hazelcastInstance;
    private final Map<String, Cache> cacheMap = new ConcurrentHashMap<>(16);

    public HazelcastCacheManager(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public Collection<String> getCacheNames() {
        return Collections.unmodifiableSet(this.cacheMap.keySet());
    }

    @Override
    @Nullable
    public Cache getCache(String name) {
        return this.cacheMap.computeIfAbsent(name, cacheName -> new HazelcastCache(cacheName, hazelcastInstance));
    }
}