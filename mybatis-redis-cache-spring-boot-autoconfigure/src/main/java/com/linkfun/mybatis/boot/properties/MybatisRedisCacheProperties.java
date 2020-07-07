package com.linkfun.mybatis.boot.properties;

import com.linkfun.mybatis.cache.redis.Mode;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-05
 * Time: 18:20
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "mybatis.cache.redis")
public class MybatisRedisCacheProperties {

    private String uri = "redis://localhost:6379/6";
    private Codec codec = Codec.kryo;
    private Mode mode = Mode.standalone;
    private Pool pool;

    @Getter
    @Setter
    public static class Pool {
        private int maxIdle = 8;
        private int minIdle = 0;
        private int maxActive = 8;
        private long maxWaitMillis = -1;
        private long timeBetweenEvictionRunsMillis = -1;
    }
}

