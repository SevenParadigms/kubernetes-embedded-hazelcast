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

import static java.time.temporal.ChronoUnit.MILLIS;

public class GuidedCache implements Cache {
    private final String cacheName;
    private final ConcurrentMap<Object, Object> cache;
    private final Integer accessExpire;
    private final Integer writeExpire;
    private final Integer maxSize;
    private final ConcurrentMap<Object, LocalDateTime> accessDelta = new ConcurrentHashMap<>();
    private final ConcurrentMap<Object, LocalDateTime> writeDelta = new ConcurrentHashMap<>();

    public GuidedCache(String cacheName, ConcurrentMap<Object, Object> cache) {
        this(cacheName, cache, Tuples.of("-1", "-1", "-1"));
    }

    public GuidedCache(String cacheName, ConcurrentMap<Object, Object> cache, Tuple3<String, String, String> properties) {
        this.cacheName = cacheName;
        this.cache = cache;
        this.accessExpire = Integer.parseInt(properties.getT1());
        this.writeExpire = Integer.parseInt(properties.getT2());
        this.maxSize = Integer.parseInt(properties.getT3());
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
        if (accessExpire > 0 && accessDelta.containsKey(key) && LocalDateTime.now().isAfter(accessDelta.get(key).plus(accessExpire, MILLIS))
                || writeExpire > 0 && writeDelta.containsKey(key) && LocalDateTime.now().isAfter(writeDelta.get(key).plus(writeExpire, MILLIS))) {
            evict(key);
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
        evict(key);
        this.cache.put(key, value);
        if (accessExpire > 0) {
            accessDelta.put(key, LocalDateTime.now());
        }
        if (writeExpire > 0) {
            writeDelta.put(key, LocalDateTime.now());
        }
        if (cache.size() > maxSize) {
            Object oldestKey = null;
            LocalDateTime oldestTime = LocalDateTime.now();
            for (var entry : accessDelta.entrySet()) {
                if (oldestTime.isAfter(entry.getValue())) {
                    oldestKey = entry.getKey();
                    oldestTime = entry.getValue();
                }
            }
            assert oldestKey != null;
            evict(oldestKey);
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
        this.accessDelta.remove(key);
        this.writeDelta.remove(key);
    }

    @Override
    public boolean evictIfPresent(@NonNull final Object key) {
        var result = this.cache.remove(key) != null;
        if (result) {
            evict(key);
        }
        return result;
    }

    @Override
    public void clear() {
        this.cache.clear();
        this.accessDelta.clear();
        this.writeDelta.clear();
    }

    @Override
    public boolean invalidate() {
        boolean notEmpty = (this.cache.size() > 0);
        clear();
        return notEmpty;
    }
}
