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
import org.apache.ibatis.cache.Cache;

/**
 * Cache adapter for Redis.
 *
 * @author Eduardo Macarron
 */
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
        redisConfig = RedisConfigurationBuilder.getInstance().parseConfiguration();
        pool = new RedisConnectionPool(redisConfig);
    }


    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public int getSize() {
        final StatefulRedisConnection<String, Object> kryo = pool.getConnection(StatefulRedisConnection.class);
        return kryo.sync().hlen(id).intValue();
    }

    @Override
    public void putObject(final Object key, final Object value) {
        final StatefulRedisConnection<String, Object> kryo = pool.getConnection(StatefulRedisConnection.class);
        kryo.sync().hset(id, key.toString(), value);
        if (timeout != null && kryo.sync().ttl(id) == -1) {
            kryo.sync().expire(id, timeout);
        }
    }

    @Override
    public Object getObject(final Object key) {
        final StatefulRedisConnection<String, Object> kryo = pool.getConnection(StatefulRedisConnection.class);
        return kryo.sync().hget(id, key.toString());
    }

    @Override
    public Object removeObject(final Object key) {
        final StatefulRedisConnection<String, Object> kryo = pool.getConnection(StatefulRedisConnection.class);
        kryo.sync().hdel(id, key.toString());
        return null;
    }

    @Override
    public void clear() {
        final StatefulRedisConnection<String, Object> kryo = pool.getConnection(StatefulRedisConnection.class);
        kryo.sync().del(id);

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
