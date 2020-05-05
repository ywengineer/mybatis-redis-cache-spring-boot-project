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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import com.linkfun.mybatis.cache.redis.codec.KryoCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.cache.CacheException;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;

/**
 * Converter from the Config to a proper {@link RedisConfig}.
 *
 * @author Eduardo Macarron
 */
@Slf4j
final class RedisConfigurationBuilder {

    /**
     * This class instance.
     */
    private static final RedisConfigurationBuilder INSTANCE = new RedisConfigurationBuilder();

    protected static final String SYSTEM_PROPERTY_REDIS_PROPERTIES_FILENAME = "redis.properties.filename";

    protected static final String REDIS_RESOURCE = "redis.properties";

    /**
     * Hidden constructor, this class can't be instantiated.
     */
    private RedisConfigurationBuilder() {
    }

    /**
     * Return this class instance.
     *
     * @return this class instance.
     */
    public static RedisConfigurationBuilder getInstance() {
        return INSTANCE;
    }

    /**
     * Parses the Config and builds a new {@link RedisConfig}.
     *
     * @return the converted {@link RedisConfig}.
     */
    public RedisConfig parseConfiguration() {
        return parseConfiguration(getClass().getClassLoader());
    }

    public RedisConfig parseConfiguration(ClassLoader classLoader) {
        final Properties config = new Properties(System.getProperties());
        String redisPropertiesFilename = System.getProperty(SYSTEM_PROPERTY_REDIS_PROPERTIES_FILENAME, REDIS_RESOURCE);
        try (InputStream input = classLoader.getResourceAsStream(redisPropertiesFilename)) {
            config.load(input);
        } catch (IOException e) {
            log.warn("An error occurred while reading classpath property '" + redisPropertiesFilename + "', see nested exceptions", e);
        }
        RedisConfig redisConfig = new RedisConfig();
        setConfigProperties(config, redisConfig);
        return redisConfig;
    }

    private void setConfigProperties(Properties properties, RedisConfig redisConfig) {
        if (properties != null) {
            MetaObject metaCache = SystemMetaObject.forObject(redisConfig);
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String name = (String) entry.getKey();
                // All prefix of 'redis.' on property values
                if (name != null && name.startsWith("redis.")) {
                    name = name.substring(6);
                } else {
                    // Skip non prefixed properties
                    continue;
                }
                String value = (String) entry.getValue();
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
