/**
 * Copyright 2015-2018 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkfun.mybatis.cache.redis;

import java.util.concurrent.locks.ReadWriteLock;

import com.linkfun.mybatis.cache.redis.conn.RedisConnectionPool;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.cache.Cache;

/**
 * Cache adapter for Redis.
 *
 * @author Eduardo Macarron
 */
@Slf4j
public final class RedisCache implements Cache {

    private final ReadWriteLock readWriteLock = new DummyReadWriteLock();

    private String id;

    private static RedisConnectionPool pool;

    private final RedisConfig redisConfig;

    private Integer timeout;

    public RedisCache(final String id) {
        if (id == null) {
            throw new IllegalArgumentException("Cache instances require an ID");
        }
        this.id = id;
        redisConfig = RedisConfigurationBuilder.INSTANCE.parseConfiguration();
        pool = new RedisConnectionPool(redisConfig);
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
                return conn.sync().hlen(id).intValue();
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                return conn.sync().hlen(id).intValue();
            }
        }.call();
    }

    @Override
    public void putObject(final Object key, final Object value) {
        new CacheJob(pool) {
            @Override
            protected Object inStandalone(StatefulRedisConnection<String, Object> conn) {
                conn.sync().hset(id, key.toString(), value);
                if (timeout != null && conn.sync().ttl(id) == -1) {
                    conn.sync().expire(id, timeout);
                }
                return null;
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                conn.sync().hset(id, key.toString(), value);
                if (timeout != null && conn.sync().ttl(id) == -1) {
                    conn.sync().expire(id, timeout);
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
                return conn.sync().hget(id, key.toString());
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                return conn.sync().hget(id, key.toString());
            }
        }.call();
    }

    @Override
    public Object removeObject(final Object key) {
        return new CacheJob(pool) {
            @Override
            protected Object inStandalone(StatefulRedisConnection<String, Object> conn) {
                return conn.sync().hdel(id, key.toString());
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                return conn.sync().hdel(id, key.toString());
            }
        }.call();
    }

    @Override
    public void clear() {
        new CacheJob(pool) {
            @Override
            protected Object inStandalone(StatefulRedisConnection<String, Object> conn) {
                return conn.sync().del(id);
            }

            @Override
            protected Object inCluster(StatefulRedisClusterConnection<String, Object> conn) {
                return conn.sync().del(id);
            }
        }.call();
    }

    @Override
    public ReadWriteLock getReadWriteLock() {
        return readWriteLock;
    }

    @Override
    public String toString() {
        return "Redis {" + id + "}";
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

}
