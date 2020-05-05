package com.linkfun.mybatis.cache.redis.conn;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import com.linkfun.mybatis.cache.redis.Mode;
import com.linkfun.mybatis.cache.redis.RedisConfig;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-04
 * Time: 22:55
 */
class RedisUtils {

    static <T> CompletableFuture<T> failed(Throwable throwable) {

        Objects.requireNonNull(throwable, "Throwable must not be null!");

        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);

        return future;
    }

    static <T> T join(CompletionStage<T> future) throws RuntimeException, CompletionException {

        Objects.requireNonNull(future, "CompletableFuture must not be null!");

        try {
            return future.toCompletableFuture().join();
        } catch (Exception e) {
            throw new CompletionException(e);
        }
    }

    static <T> Function<Throwable, T> ignoreErrors() {
        return ignored -> null;
    }

    public static AbstractRedisClient client(RedisConfig config) {
        AbstractRedisClient client;
        if (config.getUri() == null || config.getUri().length() == 0) {
            config.setUri("redis://localhost:6379/0");
        }
        if (config.getMode() == Mode.cluster) {
            client = RedisClusterClient.create(CLIENT_RESOURCES, config.getRedisURI());
            ((RedisClusterClient) client).setOptions(ClusterClientOptions.builder()
                    .autoReconnect(true)
                    .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                    .build()
            );
        } else {
            client = RedisClient.create(CLIENT_RESOURCES, config.getRedisURI());
            ((RedisClient) client).setOptions(ClientOptions.builder()
                    .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                    .build()
            );
        }
        return client;
    }

    private static final ClientResources CLIENT_RESOURCES = DefaultClientResources.create();
}
