
package com.linkfun.mybatis.cache.redis.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import io.lettuce.core.codec.RedisCodec;
import org.apache.ibatis.cache.CacheException;

public enum JDKCodec implements RedisCodec<String, Object> {
    // Enum singleton, which is preferred approach since Java 1.5
    INSTANCE;

    private final Charset charset = StandardCharsets.UTF_8;

    JDKCodec() {
        // prevent instantiation
    }

    /**
     * Decode the key output by redis.
     *
     * @param bytes Raw bytes of the key, must not be {@literal null}.
     * @return The decoded key, may be {@literal null}.
     */
    @Override
    public String decodeKey(ByteBuffer bytes) {
        return Strings.decode(bytes, charset);
    }

    /**
     * Decode the value output by redis.
     *
     * @param bytes Raw bytes of the value, must not be {@literal null}.
     * @return The decoded value, may be {@literal null}.
     */
    @Override
    public Object decodeValue(ByteBuffer bytes) {
        if (bytes == null) {
            return null;
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(Strings.readAll(bytes));
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return ois.readObject();
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    /**
     * Encode the key for output to redis.
     *
     * @param key the key, may be {@literal null}.
     * @return The encoded key, never {@literal null}.
     */
    @Override
    public ByteBuffer encodeKey(String key) {
        return ByteBuffer.wrap(Strings.nullEmpty(key).getBytes(charset));
    }

    /**
     * Encode the value for output to redis.
     *
     * @param value the value, may be {@literal null}.
     * @return The encoded value, never {@literal null}.
     */
    @Override
    public ByteBuffer encodeValue(Object value) {
        if (value == null) {
            value = new Object();
        }
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            return ByteBuffer.wrap(baos.toByteArray());
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }
}
