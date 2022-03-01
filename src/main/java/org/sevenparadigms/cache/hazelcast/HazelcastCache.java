package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import org.springframework.cache.Cache;
import org.springframework.lang.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

public class HazelcastCache implements Cache {
    private final String cacheName;
    private final ConcurrentMap<Object, Object> cache;

    public HazelcastCache(String cacheName, HazelcastInstance hazelcastInstance) {
        this.cacheName = cacheName;
        this.cache = hazelcastInstance.getMap(cacheName);
    }

    @Override
    public final String getName() {
        return this.cacheName;
    }

    @Override
    public final ConcurrentMap<Object, Object> getNativeCache() {
        return this.cache;
    }

    @Override
    public ValueWrapper get(Object key) {
        return () -> this.cache.get(key);
    }

    @Override
    public <T> T get(Object key, Class<T> type) {
        Object value = this.cache.get(key);
        if (value != null && type != null && !type.isInstance(value)) {
            throw new IllegalStateException(
                    "Cached value is not of required type [" + type.getName() + "]: " + value);
        }
        return (T) value;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    public <T> T get(Object key, final Callable<T> valueLoader) {
        Object element = this.cache.get(key);
        if (element != null) {
            return (T) element;
        } else {
            return loadValue(key, valueLoader);
        }
    }

    private <T> T loadValue(Object key, Callable<T> valueLoader) {
        T value;
        try {
            value = valueLoader.call();
        } catch (Throwable ex) {
            throw new ValueRetrievalException(key, valueLoader, ex);
        }
        put(key, value);
        return value;
    }

    @Override
    public void put(Object key, @Nullable Object value) {
        this.cache.put(key, value);
    }

    @Override
    @Nullable
    public ValueWrapper putIfAbsent(Object key, @Nullable final Object value) {
        Object existingElement = this.cache.putIfAbsent(key, value);
        return () -> existingElement;
    }

    @Override
    public void evict(Object key) {
        this.cache.remove(key);
    }

    @Override
    public boolean evictIfPresent(Object key) {
        return this.cache.remove(key) != null;
    }

    @Override
    public void clear() {
        this.cache.clear();
    }

    @Override
    public boolean invalidate() {
        boolean notEmpty = (this.cache.size() > 0);
        this.cache.clear();
        return notEmpty;
    }
}
