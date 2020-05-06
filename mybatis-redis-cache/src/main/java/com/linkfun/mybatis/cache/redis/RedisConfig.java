
package com.linkfun.mybatis.cache.redis;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@Getter
@Setter
public class RedisConfig extends GenericObjectPoolConfig<StatefulConnection<?, ?>> {

    private String uri;
    private RedisCodec<?, ?> codec;
    private Mode mode = Mode.standalone;

    public final RedisURI getRedisURI() {
        return RedisURI.create(uri);
    }
}
