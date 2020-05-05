package com.linkfun.mybatis.cache.redis;

import java.util.Properties;

import com.linkfun.mybatis.cache.redis.codec.KryoCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.cache.CacheException;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;

/**
 * Converter from the Config to a proper {@link RedisConfig}.
 *
 * @author Mark Yang
 */
@Slf4j
enum RedisConfigurationBuilder {
    INSTANCE;

    /**
     * Hidden constructor, this class can't be instantiated.
     */
    RedisConfigurationBuilder() {
    }

    /**
     * Parses the Config and builds a new {@link RedisConfig}.
     *
     * @return the converted {@link RedisConfig}.
     */
    public RedisConfig parseConfiguration() {
        final Properties config = new Properties(System.getProperties());
        final RedisConfig redisConfig = new RedisConfig();
        setConfigProperties(config, redisConfig);
        return redisConfig;
    }

    private void setConfigProperties(Properties properties, RedisConfig redisConfig) {
        if (properties != null) {
            MetaObject metaCache = SystemMetaObject.forObject(redisConfig);
            for (String name : properties.stringPropertyNames()) {
                //
                final String oriName = name;
                // All prefix of 'redis.' on property values
                if (name != null && name.startsWith("redis.")) {
                    name = name.substring(6);
                } else {
                    // Skip non prefixed properties
                    continue;
                }
                String value = properties.getProperty(oriName);
                if ("codec".equals(name)) {
                    if ("kryo".equalsIgnoreCase(value)) {
                        redisConfig.setCodec(KryoCodec.INSTANCE);
//                    } else if ("bytes".equalsIgnoreCase(value)) {
//                        redisConfig.setCodec(ByteBufferCodec.INSTANCE);
//                    } else if ("string".equalsIgnoreCase(value)) {
//                        redisConfig.setCodec(new StringCodec());
                    } else if (!"jdk".equalsIgnoreCase(value)) {
                        // Custom serializer is not supported yet.
                        throw new CacheException("Unknown serializer: '" + value + "'");
                    }
                } else if (metaCache.hasSetter(name)) {
                    Class<?> type = metaCache.getSetterType(name);
                    if (String.class == type) {
                        metaCache.setValue(name, value);
                    } else if (int.class == type || Integer.class == type) {
                        metaCache.setValue(name, Integer.valueOf(value));
                    } else if (long.class == type || Long.class == type) {
                        metaCache.setValue(name, Long.valueOf(value));
                    } else if (short.class == type || Short.class == type) {
                        metaCache.setValue(name, Short.valueOf(value));
                    } else if (byte.class == type || Byte.class == type) {
                        metaCache.setValue(name, Byte.valueOf(value));
                    } else if (float.class == type || Float.class == type) {
                        metaCache.setValue(name, Float.valueOf(value));
                    } else if (boolean.class == type || Boolean.class == type) {
                        metaCache.setValue(name, Boolean.valueOf(value));
                    } else if (double.class == type || Double.class == type) {
                        metaCache.setValue(name, Double.valueOf(value));
                    } else if (Mode.class == type) {
                        metaCache.setValue(name, Mode.valueOf(value));
                    } else {
                        throw new CacheException("Unsupported property type: '" + name + "' of type " + type);
                    }
                }
            }
        }
    }
}
