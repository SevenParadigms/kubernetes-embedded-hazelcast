package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.cache.ICache;
import com.hazelcast.map.IMap;
import org.springframework.cache.Cache;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class GuidedCache implements Cache {
    private final String cacheName;
    private ICache<Object, Object> cache;
    private final IMap<Object, LocalDateTime> maxDelta;
    private final Integer accessExpire;
    private final Integer writeExpire;
    private final Integer maxSize;

    public GuidedCache(String cacheName, ICache<Object, Object> cache, IMap<Object, LocalDateTime> maxDelta,
                       Integer accessExpire, Integer writeExpire, Integer maxSize) {
        this.cacheName = cacheName;
        this.cache = cache;
        this.maxDelta = maxDelta;
        this.accessExpire = accessExpire;
        this.writeExpire = writeExpire;
        this.maxSize = maxSize;
    }

    private ExpiryPolicy expiryPolicy() {
        if (accessExpire > 0) {
            return new AccessedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, accessExpire));
        } else if (writeExpire > 0) {
            return new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, writeExpire));
        }
        return null;
    }

    @Override
    @NonNull
    public final String getName() {
        return this.cacheName;
    }

    @Override
    @NonNull
    public final ICache<Object, Object> getNativeCache() {
        return this.cache;
    }

    @Override
    @NonNull
    public ValueWrapper get(@NonNull final Object key) {
        return () -> cache.get(key, expiryPolicy());
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
        cache.put(key, value, expiryPolicy());
        maxDelta.put(key, LocalDateTime.now());
        maxDelta.forEach((k, v) -> {
            if (!cache.containsKey(k)) {
                maxDelta.remove(k);
            }
        });
        if (cache.size() > maxSize) {
            final AtomicReference<Object> oldestKey = new AtomicReference<>();
            final AtomicReference<LocalDateTime> oldestTime = new AtomicReference<>(LocalDateTime.now());
            maxDelta.forEach((k, v) -> {
                if (oldestTime.get().isAfter(v)) {
                    oldestKey.set(k);
                    oldestTime.set(v);
                }
            });
            evict(oldestKey.get());
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
        this.maxDelta.remove(key);
    }

    @Override
    public boolean evictIfPresent(@NonNull final Object key) {
        var result = this.cache.remove(key);
        if (result) {
            evict(key);
        }
        return result;
    }

    @Override
    public void clear() {
        this.cache.clear();
    }

    @Override
    public boolean invalidate() {
        boolean notEmpty = (this.cache.size() > 0);
        clear();
        return notEmpty;
    }
}
