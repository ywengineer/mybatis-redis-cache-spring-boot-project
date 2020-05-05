package com.linkfun.mybatis.cache.redis;

import java.util.concurrent.Callable;

import com.linkfun.mybatis.cache.redis.conn.RedisConnectionPool;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-05
 * Time: 12:21
 */
public abstract class CacheJob implements Callable<Object> {
    private RedisConnectionPool pool;

    public CacheJob(RedisConnectionPool pool) {
        Assert.notNull(pool, "redis connection pool must be non null");
        this.pool = pool;
    }

    protected abstract Object inStandalone(StatefulRedisConnection<String, Object> conn);

    protected abstract Object inCluster(StatefulRedisClusterConnection<String, Object> conn);

    @Override
    public final Object call() {
        StatefulConnection<String, Object> conn = this.getConn();
        if (conn instanceof StatefulRedisClusterConnection) {
            return inCluster((StatefulRedisClusterConnection<String, Object>) conn);
        }
        return inStandalone((StatefulRedisConnection<String, Object>) conn);
    }

    private StatefulConnection<String, Object> getConn() {
        if (pool.getPoolConfig().getMode() == Mode.standalone) {
            //
            if (CollectionUtils.isEmpty(pool.getPoolConfig().getRedisURI().getSentinels())) {
                return pool.getConnection(StatefulRedisConnection.class);
            }
            return pool.getConnection(StatefulRedisMasterReplicaConnection.class);
        }
        return pool.getConnection(StatefulRedisClusterConnection.class);
    }
}
