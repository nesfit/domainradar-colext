package cz.vut.fit.domainradar;


import java.io.Closeable;
import java.util.concurrent.*;

public class ExpiringConcurrentCache<K, V> implements Closeable {
    private static class Entry<V> {
        long _lastAccessed;
        V _value;

        Entry(V value) {
            _lastAccessed = System.nanoTime();
            _value = value;
        }

        V getValue() {
            _lastAccessed = System.nanoTime();
            return _value;
        }
    }

    private final ConcurrentMap<K, Entry<V>> _cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService _scheduler = Executors.newScheduledThreadPool(1);
    private final long _lifetime;

    public ExpiringConcurrentCache(long entryLifetime, long checkInterval, TimeUnit timeUnit) {
        _lifetime = timeUnit.toNanos(entryLifetime);
        _scheduler.scheduleAtFixedRate(this::removeExpiredEntries, checkInterval, checkInterval, timeUnit);
    }

    public V get(K key) {
        var entry = _cache.get(key);
        if (entry != null) {
            return entry.getValue();
        }
        return null;
    }

    public V put(K key, V value) {
        _cache.put(key, new Entry<>(value));
        return value;
    }

    public V putIfAbsent(K key, V value) {
        var entry = _cache.putIfAbsent(key, new Entry<>(value));
        return entry == null ? value : entry._value;
    }


    private void removeExpiredEntries() {
        long expirationThreshold = System.nanoTime() - _lifetime;
        for (K key : _cache.keySet()) {
            var value = _cache.get(key);
            if (value == null)
                continue;
            if (value._lastAccessed < expirationThreshold) {
                _cache.remove(key);
            }
        }
    }

    @Override
    public void close() {
        _scheduler.shutdown();
    }
}