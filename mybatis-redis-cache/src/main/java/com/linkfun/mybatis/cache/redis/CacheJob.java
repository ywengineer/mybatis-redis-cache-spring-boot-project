package com.linkfun.mybatis.cache.redis;

import java.util.Objects;
import java.util.concurrent.Callable;

import com.linkfun.mybatis.cache.redis.conn.RedisConnectionPool;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-05
 * Time: 12:21
 */
public abstract class CacheJob implements Callable<Object> {
    private final RedisConnectionPool pool;

    public CacheJob(RedisConnectionPool pool) {
        Objects.requireNonNull(pool, "redis connection pool must be non null");
        this.pool = pool;
    }

    protected abstract Object inStandalone(StatefulRedisConnection<String, Object> conn);

    protected abstract Object inCluster(StatefulRedisClusterConnection<String, Object> conn);

    @Override
    public final Object call() {
        //
        final StatefulConnection<String, Object> conn = this.getConn();
        try {
            if (conn instanceof StatefulRedisClusterConnection) {
                return inCluster((StatefulRedisClusterConnection<String, Object>) conn);
            }
            return inStandalone((StatefulRedisConnection<String, Object>) conn);
        } finally {
            pool.release(conn);
        }
    }

    private StatefulConnection<String, Object> getConn() {
        if (pool.getPoolConfig().getMode() == Mode.standalone) {
            //
            if (pool.getPoolConfig().getRedisURI().getSentinels().size() > 0) {
                return pool.getConnection(StatefulRedisMasterReplicaConnection.class);
            }
            return pool.getConnection(StatefulRedisConnection.class);
        }
        return pool.getConnection(StatefulRedisClusterConnection.class);
    }
}
