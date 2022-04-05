package org.sevenparadigms.cache.hazelcast;

import org.springframework.cache.Cache;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.time.temporal.ChronoUnit.SECONDS;

public class GuidedCache implements Cache {
    private final String cacheName;
    private final ConcurrentMap<Object, Object> cache;
    private final Integer accessExpireSeconds;
    private final Integer writeExpireSeconds;
    private final Integer maximumSizeCount;
    private final ConcurrentMap<Object, LocalDateTime> deltaStore = new ConcurrentHashMap<>();

    public GuidedCache(String cacheName, ConcurrentMap<Object, Object> cache) {
        this(cacheName, cache, Tuples.of("-1", "-1", "-1"));
    }

    public GuidedCache(String cacheName, ConcurrentMap<Object, Object> cache, Tuple3<String, String, String> properties) {
        this.cacheName = cacheName;
        this.cache = cache;
        this.accessExpireSeconds = Integer.parseInt(properties.getT1());
        this.writeExpireSeconds = Integer.parseInt(properties.getT2());
        this.maximumSizeCount = Integer.parseInt(properties.getT3());
    }

    @Override
    @NonNull
    public final String getName() {
        return this.cacheName;
    }

    @Override
    @NonNull
    public final ConcurrentMap<Object, Object> getNativeCache() {
        return this.cache;
    }

    @Override
    @NonNull
    public ValueWrapper get(@NonNull final Object key) {
        if (accessExpireSeconds > 0 || writeExpireSeconds > 0) {
            var delta = deltaStore.get(key);
            if (accessExpireSeconds > 0 && LocalDateTime.now().isAfter(delta.plus(accessExpireSeconds, SECONDS))
                    || writeExpireSeconds > 0 && LocalDateTime.now().isAfter(delta.plus(writeExpireSeconds, SECONDS))) {
                evict(key);
            }
        }
        return () -> this.cache.get(key);
    }

    @Override
    @Nullable
    public <T> T get(@NonNull final Object key, @Nullable Class<T> type) {
        ValueWrapper value = get(key);
        if (value.get() != null && type != null && !type.isInstance(value.get())) {
            throw new IllegalStateException(
                    "Cached value do not have required type [" + type.getName() + "]: " + value);
        }
        return (T) value.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    public <T> T get(@NonNull final Object key, @NonNull final Callable<T> valueLoader) {
        ValueWrapper value = get(key);
        if (value.get() != null) {
            return (T) value.get();
        } else {
            return loadValue(key, valueLoader);
        }
    }

    private <T> T loadValue(final Object key, final Callable<T> valueLoader) {
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
    public void put(@NonNull final Object key, @Nullable final Object value) {
        if (maximumSizeCount > 0 && maximumSizeCount == cache.size()) {
            Object oldestKey = null;
            LocalDateTime oldest = LocalDateTime.now();
            for (var entry : deltaStore.entrySet()) {
                if (entry.getValue().isBefore(oldest)) {
                    oldestKey = entry.getKey();
                }
            }
            assert oldestKey != null;
            evict(oldestKey);
        }
        evict(key);
        this.cache.put(key, value);
        if (accessExpireSeconds > 0 || writeExpireSeconds > 0) {
            deltaStore.put(key, LocalDateTime.now());
        }
    }

    @Override
    @Nullable
    public ValueWrapper putIfAbsent(@NonNull final Object key, @Nullable final Object value) {
        if (!cache.containsKey(key)) {
            put(key, value);
        }
        return () -> value;
    }

    @Override
    public void evict(@NonNull final Object key) {
        this.cache.remove(key);
        this.deltaStore.remove(key);
    }

    @Override
    public boolean evictIfPresent(@NonNull final Object key) {
        var result = this.cache.remove(key) != null;
        if (result) {
            this.deltaStore.remove(key);
        }
        return result;
    }

    @Override
    public void clear() {
        this.cache.clear();
        this.deltaStore.clear();
    }

    @Override
    public boolean invalidate() {
        boolean notEmpty = (this.cache.size() > 0);
        this.cache.clear();
        this.deltaStore.clear();
        return notEmpty;
    }
}
