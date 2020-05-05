package com.linkfun.mybatis.boot.properties;

import com.linkfun.mybatis.cache.redis.Mode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
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
    private String codec = "kryo";
    private Mode mode = Mode.standalone;
    private GenericObjectPoolConfig<?> pool;
}

