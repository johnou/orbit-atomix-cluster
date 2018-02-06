package com.sulake.orbit.cluster;

import cloud.orbit.actors.cluster.NodeAddress;
import cloud.orbit.actors.runtime.RemoteKey;
import io.atomix.core.Atomix;
import io.atomix.core.map.ConsistentMap;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.Persistence;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Johno Crawford (johno@sulake.com)
 */
public class AtomixConcurrentMap<K, V> implements ConcurrentMap<K, V> {

    private final ConsistentMap<K, V> cache;

    /**
     * Creates a new Orbit compatible serializer.
     */
    private static Serializer createSerializer() {
        return Serializer.using(KryoNamespace.builder()
                .setRegistrationRequired(false)
                .register(KryoNamespaces.BASIC)
                .register(RemoteKey.class)
                .register(NodeAddress.class)
                .build());
    }

    AtomixConcurrentMap(Atomix atomix, String id) {
        this.cache = atomix.<K, V>consistentMapBuilder(id)
                .withSerializer(createSerializer())
                .withConsistency(Consistency.LINEARIZABLE)
                .withPersistence(Persistence.PERSISTENT)
                .withReplication(Replication.SYNCHRONOUS)
                .withRecovery(Recovery.RECOVER)
                .withMaxRetries(5)
                .build();
    }

    @Override
    public V putIfAbsent(K k, V v) {
        Versioned<V> value = cache.putIfAbsent(k, v);
        return value != null ? value.value() : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object k, Object v) {
        return cache.remove((K) k, (V) v);
    }

    @Override
    public boolean replace(K k, V v, V v1) {
        return cache.replace(k, v, v1);
    }

    @Override
    public V replace(K k, V v) {
        Versioned<V> value = cache.replace(k, v);
        return value != null ? value.value() : null;
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean containsKey(Object o) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean containsValue(Object o) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object o) {
        Versioned<V> value = cache.get((K) o);
        return value != null ? value.value() : null;
    }

    @Override
    public V put(K k, V v) {
        Versioned<V> value = cache.put(k, v);
        return value != null ? value.value() : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(Object o) {
        Versioned<V> value = cache.remove((K) o);
        return value != null ? value.value() : null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("Not supported");
    }
}
