package org.sevenparadigms.cache.hazelcast;

import com.hazelcast.cache.ICache;
import com.hazelcast.map.IMap;
import lombok.SneakyThrows;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.springframework.cache.Cache;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import javax.cache.expiry.ExpiryPolicy;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.springframework.data.r2dbc.repository.query.Dsl.COLON;

public class GuidedCache implements Cache {
    private final String cacheName;
    private final ICache<Object, Object> cache;
    private final IMap<String, LocalDateTime> maxDelta;
    private final String cacheLock;
    private final ExpiryPolicy expiryPolicy;
    private final Integer maxSize;

    private final AtomicReference<LocalDateTime> resetDelta = new AtomicReference<>(LocalDateTime.now());
    private final AtomicInteger lastSize = new AtomicInteger();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public GuidedCache(String cacheName, ICache<Object, Object> cache, IMap<String, LocalDateTime> maxDelta,
                       ExpiryPolicy expiryPolicy, Integer maxSize) {
        this.cacheName = cacheName;
        this.cache = cache;
        this.maxDelta = maxDelta;
        this.cacheLock = getClass().getName() + COLON + cacheName;
        this.expiryPolicy = expiryPolicy;
        this.maxSize = maxSize;
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
        return () -> cache.get(key, expiryPolicy);
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
    @SneakyThrows
    public void put(@NonNull final Object key, @Nullable final Object value) {
        evict(key);
        cache.put(key, value, expiryPolicy);
        maxDelta.put(key.toString(), LocalDateTime.now());
        if (!maxDelta.isLocked(cacheLock)) {
            if (LocalDateTime.now().isAfter(resetDelta.get().plus(500, MILLIS))) {
                executorService.submit(() -> {
                    maxDelta.lock(cacheLock, 250, MILLISECONDS);
                    Set<Object> removeKeys = new HashSet<>();
                    maxDelta.forEach((k, v) -> {
                        if (!Objects.equals(k, cacheLock) && !cache.containsKey(k)) removeKeys.add(k);
                    });
                    removeKeys.forEach(maxDelta::remove);
                    lastSize.set(cache.size());
                    if (lastSize.get() > maxSize) {
                        resolveMax(false).run();
                    } else {
                        maxDelta.forceUnlock(cacheLock);
                    }
                });
                resetDelta.set(LocalDateTime.now());
            } else {
                if (lastSize.incrementAndGet() > maxSize) {
                    if (lastSize.get() < 250) {
                        resolveMax(true).run();
                    } else if (LocalDateTime.now().isAfter(resetDelta.get().plus(250, MILLIS))) {
                        executorService.submit(resolveMax(true));
                        resetDelta.set(LocalDateTime.now());
                    }
                }
            }
        } else {
            lastSize.incrementAndGet();
            resetDelta.set(LocalDateTime.now());
        }
    }

    private Runnable resolveMax(boolean isLocking) {
        return () -> {
            try {
                if (isLocking) maxDelta.lock(cacheLock, 250, MILLISECONDS);
                CircularFifoQueue<Object> evictedKeys = new CircularFifoQueue<>(lastSize.get() - maxSize);
                final AtomicReference<LocalDateTime> oldestTime = new AtomicReference<>(LocalDateTime.now());
                maxDelta.forEach((k, v) -> {
                    if (oldestTime.get().isAfter(v)) {
                        evictedKeys.add(k);
                        oldestTime.set(v);
                    }
                });
                evictedKeys.forEach(this::evict);
            } finally {
                maxDelta.forceUnlock(cacheLock);
            }
        };
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
        evictIfPresent(key);
    }

    @Override
    public boolean evictIfPresent(@NonNull final Object key) {
        var result = this.cache.remove(key);
        if (result) {
            this.lastSize.decrementAndGet();
            this.maxDelta.remove(key);
        }
        return result;
    }

    @Override
    public void clear() {
        this.lastSize.set(0);
        this.cache.clear();
    }

    @Override
    public boolean invalidate() {
        boolean notEmpty = (this.cache.size() > 0);
        clear();
        return notEmpty;
    }
}
