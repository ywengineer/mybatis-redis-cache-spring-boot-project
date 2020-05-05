/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkfun.mybatis.cache.redis.conn;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import com.linkfun.mybatis.cache.redis.RedisConfig;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.support.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.PoolException;
import org.springframework.util.Assert;

@Slf4j
public class RedisConnectionPool implements DisposableBean, Closeable {
    private final RedisConfig poolConfig;
    private final Map<StatefulConnection<?, ?>, GenericObjectPool<StatefulConnection<?, ?>>> poolRef = new ConcurrentHashMap<>(
            32);

    private final Map<StatefulConnection<?, ?>, AsyncPool<StatefulConnection<?, ?>>> asyncPoolRef = new ConcurrentHashMap<>(
            32);
    private final Map<CompletableFuture<StatefulConnection<?, ?>>, AsyncPool<StatefulConnection<?, ?>>> inProgressAsyncPoolRef = new ConcurrentHashMap<>(
            32);
    private final Map<Class<?>, GenericObjectPool<StatefulConnection<?, ?>>> pools = new ConcurrentHashMap<>(32);
    private final Map<Class<?>, AsyncPool<StatefulConnection<?, ?>>> asyncPools = new ConcurrentHashMap<>(32);
    private final BoundedPoolConfig asyncPoolConfig;
    private final AbstractRedisClient client;
    private final RedisURI redisURI;
    private boolean clusterInitialized;

    public RedisConnectionPool(RedisConfig poolConfig) {
        Assert.notNull(poolConfig, "RedisConfig must not be null!");
        this.client = RedisUtils.client(poolConfig);
        this.redisURI = poolConfig.getRedisURI();
        this.poolConfig = poolConfig;
        this.asyncPoolConfig = CommonsPool2ConfigConverter.bounded(this.poolConfig);
    }

    public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {
        GenericObjectPool<StatefulConnection<?, ?>> pool = pools.computeIfAbsent(connectionType, poolType -> {
            return ConnectionPoolSupport.createGenericObjectPool(() -> _getConnection(connectionType), poolConfig, false);
        });

        try {
            StatefulConnection<?, ?> connection = pool.borrowObject();
            poolRef.put(connection, pool);
            return connectionType.cast(connection);
        } catch (Exception e) {
            throw new PoolException("Could not get a resource from the pool", e);
        }
    }

    public <T extends StatefulConnection<?, ?>, K, V> CompletionStage<T> getConnectionAsync(Class<T> connectionType) {

        AsyncPool<StatefulConnection<?, ?>> pool = asyncPools.computeIfAbsent(connectionType, poolType -> AsyncConnectionPoolSupport.createBoundedObjectPool(
                () -> _getConnectionAsync(connectionType).thenApply(connectionType::cast), asyncPoolConfig,
                false));

        CompletableFuture<StatefulConnection<?, ?>> acquire = pool.acquire();

        inProgressAsyncPoolRef.put(acquire, pool);
        return acquire.whenComplete((connection, e) -> {

            inProgressAsyncPoolRef.remove(acquire);

            if (connection != null) {
                asyncPoolRef.put(connection, pool);
            }
        }).thenApply(connectionType::cast);
    }

    private <T extends StatefulConnection<?, ?>> T _getConnection(Class<T> connectionType) {
        if (client instanceof RedisClient) {
            if (connectionType.equals(StatefulRedisMasterReplicaConnection.class)) {
                return connectionType.cast(MasterReplica.connect(((RedisClient) client), this.poolConfig.getCodec(), this.redisURI));
            }
            if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
                return connectionType.cast(((RedisClient) client).connectPubSub(this.poolConfig.getCodec()));
            }
            if (StatefulConnection.class.isAssignableFrom(connectionType)) {
                return connectionType.cast(((RedisClient) client).connect(this.poolConfig.getCodec()));
            }
        } else {
            return RedisUtils.join(getConnectionAsync(connectionType));
        }
        throw new UnsupportedOperationException("Connection type " + connectionType + " not supported!");
    }

    private <T extends StatefulConnection<?, ?>> CompletionStage<T> _getConnectionAsync(Class<T> connectionType) {

        if (client instanceof RedisClient) {
            RedisClient redisClient = ((RedisClient) client);
            if (connectionType.equals(StatefulRedisMasterReplicaConnection.class)) {
                return MasterReplica.connectAsync((RedisClient) client, this.poolConfig.getCodec(), redisURI).thenApply(connectionType::cast);
            }
            if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
                return redisClient.connectPubSubAsync(this.poolConfig.getCodec(), redisURI).thenApply(connectionType::cast);
            }
            if (StatefulConnection.class.isAssignableFrom(connectionType)) {
                return redisClient.connectAsync(this.poolConfig.getCodec(), redisURI).thenApply(connectionType::cast);
            }
        } else {
            if (!clusterInitialized) {
                // partitions have to be initialized before asynchronous usage.
                // Needs to happen only once. Initialize eagerly if
                // blocking is not an options.
                synchronized (this) {
                    if (!clusterInitialized) {
                        ((RedisClusterClient) client).getPartitions();
                        clusterInitialized = true;
                    }
                }
            }

            if (connectionType.equals(StatefulRedisPubSubConnection.class)
                    || connectionType.equals(StatefulRedisClusterPubSubConnection.class)) {

                return ((RedisClusterClient) client).connectPubSubAsync(this.poolConfig.getCodec()) //
                        .thenApply(connectionType::cast);
            }

            if (StatefulRedisClusterConnection.class.isAssignableFrom(connectionType)
                    || connectionType.equals(StatefulConnection.class)) {

                return ((RedisClusterClient) client).connectAsync(this.poolConfig.getCodec()) //
                        .thenApply(connectionType::cast);
            }

        }

        return RedisUtils.failed(new UnsupportedOperationException("Connection type " + connectionType + " not supported!"));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#release(io.lettuce.core.api.StatefulConnection)
     */
    public void release(StatefulConnection<?, ?> connection) {

        GenericObjectPool<StatefulConnection<?, ?>> pool = poolRef.remove(connection);

        if (pool == null) {

            AsyncPool<StatefulConnection<?, ?>> asyncPool = asyncPoolRef.remove(connection);

            if (asyncPool == null) {
                throw new PoolException("Returned connection " + connection
                        + " was either previously returned or does not belong to this connection provider");
            }

            discardIfNecessary(connection);
            asyncPool.release(connection).join();
            return;
        }

        discardIfNecessary(connection);
        pool.returnObject(connection);
    }

    private void discardIfNecessary(StatefulConnection<?, ?> connection) {

        if (connection instanceof StatefulRedisConnection) {

            StatefulRedisConnection<?, ?> redisConnection = (StatefulRedisConnection<?, ?>) connection;
            if (redisConnection.isMulti()) {
                redisConnection.async().discard();
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#releaseAsync(io.lettuce.core.api.StatefulConnection)
     */
    public CompletableFuture<Void> releaseAsync(StatefulConnection<?, ?> connection) {

        GenericObjectPool<StatefulConnection<?, ?>> blockingPool = poolRef.remove(connection);

        if (blockingPool != null) {

            log.warn("Releasing asynchronously a connection that was obtained from a non-blocking pool");
            blockingPool.returnObject(connection);
            return CompletableFuture.completedFuture(null);
        }

        AsyncPool<StatefulConnection<?, ?>> pool = asyncPoolRef.remove(connection);

        if (pool == null) {
            return RedisUtils.failed(new PoolException("Returned connection " + connection
                    + " was either previously returned or does not belong to this connection provider"));
        }

        return pool.release(connection);
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {

        List<CompletableFuture<?>> futures = new ArrayList<>();
        if (!poolRef.isEmpty() || !asyncPoolRef.isEmpty()) {
            log.warn("LettucePoolingConnectionProvider contains unreleased connections");
        }

        if (!inProgressAsyncPoolRef.isEmpty()) {

            log.warn("LettucePoolingConnectionProvider has active connection retrievals");
            inProgressAsyncPoolRef.forEach((k, v) -> futures.add(k.thenApply(StatefulConnection::closeAsync)));
        }

        if (!poolRef.isEmpty()) {

            poolRef.forEach((connection, pool) -> pool.returnObject(connection));
            poolRef.clear();
        }

        if (!asyncPoolRef.isEmpty()) {

            asyncPoolRef.forEach((connection, pool) -> futures.add(pool.release(connection)));
            asyncPoolRef.clear();
        }

        pools.forEach((type, pool) -> pool.close());

        CompletableFuture
                .allOf(futures.stream().map(it -> it.exceptionally(RedisUtils.ignoreErrors()))
                        .toArray(CompletableFuture[]::new)) //
                .thenCompose(ignored -> {

                    CompletableFuture[] poolClose = asyncPools.values().stream().map(AsyncPool::closeAsync)
                            .map(it -> it.exceptionally(RedisUtils.ignoreErrors())).toArray(CompletableFuture[]::new);

                    return CompletableFuture.allOf(poolClose);
                }) //
                .thenRun(() -> {
                    asyncPoolRef.clear();
                    inProgressAsyncPoolRef.clear();
                }) //
                .join();

        pools.clear();
    }

    public RedisConfig getPoolConfig() {
        return poolConfig;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.beans.factory.DisposableBean#destroy()
     */
    @Override
    public void destroy() throws Exception {
        this.close();
    }
}
