package com.linkfun.mybatis.cache.redis;

import java.util.concurrent.locks.ReadWriteLock;

import com.linkfun.mybatis.cache.redis.conn.RedisConnectionPool;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.cache.Cache;
import org.slf4j.helpers.MessageFormatter;

/**
 * Cache adapter for Redis.
 *
 * @author Eduardo Macarron
 */
@Slf4j
public final class RedisCache implements Cache {

    private final ReadWriteLock readWriteLock = new DummyReadWriteLock();
    private static RedisConnectionPool pool;
    //
    private final String id;
    private Integer timeout;
    private final String cacheKey;

    public RedisCache(final String id) {
        if (id == null) {
            throw new IllegalArgumentException("Cache instances require an ID");
        }
        // use share connection pool.
        if (pool == null) {
            synchronized (RedisCache.class) {
                if (pool == null) {
                    final RedisConfig redisConfig = RedisConfigParser.INSTANCE.parseConfiguration();
                    pool = new RedisConnectionPool(redisConfig);
                }
            }
        }
        this.id = id;
        this.cacheKey = MessageFormatter.format("{}_{}", id, pool.getPoolConfig().getCodec().getClass().getSimpleName()).getMessage();
        //
        if (log.isInfoEnabled()) log.info("create mybatis redis cache : {}", id);
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public int getSize() {
        return (int) new CacheJob(pool) {
            @Override
            protected Object inStandalone(StatefulRedisConnection<String, Object> conn) {
                return conn.sync().hlen(cacheKey).intValue();
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                return conn.sync().hlen(cacheKey).intValue();
            }
        }.call();
    }

    @Override
    public void putObject(final Object key, final Object value) {
        new CacheJob(pool) {
            @Override
            protected Object inStandalone(StatefulRedisConnection<String, Object> conn) {
                conn.sync().hset(cacheKey, key.toString(), value);
                if (timeout != null && conn.sync().ttl(cacheKey) == -1) {
                    conn.sync().expire(cacheKey, timeout);
                }
                return null;
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                conn.sync().hset(cacheKey, key.toString(), value);
                if (timeout != null && conn.sync().ttl(cacheKey) == -1) {
                    conn.sync().expire(cacheKey, timeout);
                }
                return null;
            }
        }.call();
    }

    @Override
    public Object getObject(final Object key) {
        return new CacheJob(pool) {
            @Override
            protected Object inStandalone(StatefulRedisConnection<String, Object> conn) {
                return conn.sync().hget(cacheKey, key.toString());
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                return conn.sync().hget(cacheKey, key.toString());
            }
        }.call();
    }

    @Override
    public Object removeObject(final Object key) {
        return new CacheJob(pool) {
            @Override
            protected Object inStandalone(StatefulRedisConnection<String, Object> conn) {
                return conn.sync().hdel(cacheKey, key.toString());
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                return conn.sync().hdel(cacheKey, key.toString());
            }
        }.call();
    }

    @Override
    public void clear() {
        new CacheJob(pool) {
            @Override
            protected Object inStandalone(StatefulRedisConnection<String, Object> conn) {
                return conn.sync().del(cacheKey);
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                return conn.sync().del(cacheKey);
            }
        }.call();
    }

    @Override
    public ReadWriteLock getReadWriteLock() {
        return readWriteLock;
    }

    @Override
    public String toString() {
        return "RedisCache{" + "id='" + id + '\'' +
                ", timeout=" + timeout +
                ", cacheKey='" + cacheKey + '\'' +
                '}';
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }
}