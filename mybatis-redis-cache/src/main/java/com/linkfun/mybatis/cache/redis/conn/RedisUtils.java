package com.linkfun.mybatis.cache.redis.conn;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.linkfun.mybatis.cache.redis.Mode;
import com.linkfun.mybatis.cache.redis.RedisConfig;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import org.springframework.data.redis.connection.lettuce.LettuceExceptionConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-04
 * Time: 22:55
 */
class RedisUtils {

    /**
     * Creates a {@link CompletableFuture} that is completed {@link CompletableFuture#exceptionally(Function)
     * exceptionally} given {@link Throwable}. This utility method allows exceptionally future creation with a single
     * invocation.
     *
     * @param throwable must not be {@literal null}.
     * @return the completed {@link CompletableFuture future}.
     */
    static <T> CompletableFuture<T> failed(Throwable throwable) {

        Assert.notNull(throwable, "Throwable must not be null!");

        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);

        return future;
    }

    /**
     * Synchronizes a {@link CompletableFuture} result by {@link CompletableFuture#join() waiting until the future is
     * complete}. This method preserves {@link RuntimeException}s that may get thrown as result of future completion.
     * Checked exceptions are thrown encapsulated within {@link java.util.concurrent.CompletionException}.
     *
     * @param future must not be {@literal null}.
     * @return the future result if completed normally.
     * @throws RuntimeException    thrown if the future is completed with a {@link RuntimeException}.
     * @throws CompletionException thrown if the future is completed with a checked exception.
     */
    @Nullable
    static <T> T join(CompletionStage<T> future) throws RuntimeException, CompletionException {

        Assert.notNull(future, "CompletableFuture must not be null!");

        try {
            return future.toCompletableFuture().join();
        } catch (Exception e) {

            Throwable exceptionToUse = e;

            if (e instanceof CompletionException) {
                exceptionToUse = new LettuceExceptionConverter().convert((Exception) e.getCause());
                if (exceptionToUse == null) {
                    exceptionToUse = e.getCause();
                }
            }

            if (exceptionToUse instanceof RuntimeException) {
                throw (RuntimeException) exceptionToUse;
            }

            throw new CompletionException(exceptionToUse);
        }
    }

    /**
     * Returns a {@link Function} that ignores {@link CompletionStage#exceptionally(Function) exceptional completion} by
     * recovering to {@code null}. This allows to progress with a previously failed {@link CompletionStage} without regard
     * to the actual success/exception state.
     *
     * @return
     */
    static <T> Function<Throwable, T> ignoreErrors() {
        return ignored -> null;
    }

    public static AbstractRedisClient client(RedisConfig config) {
        AbstractRedisClient client;
        if (StringUtils.isEmpty(config.getUri())) {
            config.setUri("redis://localhost:6379/0");
        }
        List<RedisURI> uriList = Arrays.stream(config.getUri().split(",")).map(RedisURI::create)
                .collect(Collectors.toList());
        if (uriList.size() == 1) {
            client = RedisClient.create();
            ((RedisClient) client).setOptions(ClientOptions.builder().
                    disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS).build());
        } else {
            if (config.getMode() == Mode.master_slave) {
                client = RedisClient.create();
                ((RedisClient) client).setOptions(ClientOptions.builder().
                        disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS).build());
            } else {
                client = RedisClusterClient.create(uriList);
                ((RedisClusterClient) client).setOptions(ClusterClientOptions.builder().
                        disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS).build());
            }
        }
        return client;
    }
}
