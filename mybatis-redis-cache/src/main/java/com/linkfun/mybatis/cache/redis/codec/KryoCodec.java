
package com.linkfun.mybatis.cache.redis.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.lettuce.core.codec.RedisCodec;

/**
 * SerializeUtil with Kryo, which is faster and more space consuming.
 *
 * @author Lei Jiang(ladd.cn@gmail.com)
 */
public enum KryoCodec implements RedisCodec<String, Object> {
    // Enum singleton, which is preferred approach since Java 1.5
    INSTANCE;

    private Kryo kryo;
    private Output output;
    private Input input;
    /**
     * Classes which can not resolved by default kryo serializer, which occurs very
     * rare(https://github.com/EsotericSoftware/kryo#using-standard-java-serialization) For these classes, we will use
     * fallbackSerializer(use JDKSerializer now) to resolve.
     */
    private HashSet<Class<?>> unnormalClassSet;

    /**
     * Hash codes of unnormal bytes which can not resolved by default kryo serializer, which will be resolved by
     * fallbackSerializer
     */
    private HashSet<Integer> unnormalBytesHashCodeSet;
    private RedisCodec<String, Object> fallbackSerializer;

    KryoCodec() {
        kryo = new Kryo();
        output = new Output(200, -1);
        input = new Input();
        unnormalClassSet = new HashSet<Class<?>>();
        unnormalBytesHashCodeSet = new HashSet<Integer>();
        fallbackSerializer = JDKCodec.INSTANCE;// use JDKSerializer as fallback
    }

    /**
     * Decode the key output by redis.
     *
     * @param bytes Raw bytes of the key, must not be {@literal null}.
     * @return The decoded key, may be {@literal null}.
     */
    @Override
    public String decodeKey(ByteBuffer bytes) {
        return Strings.decode(bytes, StandardCharsets.UTF_8);
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
        final byte[] b = Strings.readAll(bytes);
        int hashCode = Arrays.hashCode(b);
        if (!unnormalBytesHashCodeSet.contains(hashCode)) {
            /**
             * In the following cases: 1. This bytes occurs for the first time. 2. This bytes have occurred and can be
             * resolved by default kryo serializer
             */
            try {
                input.setBuffer(b);
                return kryo.readClassAndObject(input);
            } catch (Exception e) {
                // For unnormal bytes occurred for the first time, exception will be thrown
                unnormalBytesHashCodeSet.add(hashCode);
                return fallbackSerializer.decodeValue(bytes);// use fallback Serializer to resolve
            }
        } else {
            // For unnormal bytes
            return fallbackSerializer.decodeValue(bytes);
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
        return ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Encode the value for output to redis.
     *
     * @param object the value, may be {@literal null}.
     * @return The encoded value, never {@literal null}.
     */
    @Override
    public ByteBuffer encodeValue(Object object) {
        output.clear();
        if (!unnormalClassSet.contains(object.getClass())) {
            /**
             * In the following cases: 1. This class occurs for the first time. 2. This class have occurred and can be
             * resolved by default kryo serializer
             */
            try {
                kryo.writeClassAndObject(output, object);
                return ByteBuffer.wrap(output.toBytes());
            } catch (Exception e) {
                // For unnormal class occurred for the first time, exception will be thrown
                unnormalClassSet.add(object.getClass());
                return fallbackSerializer.encodeValue(object);// use fallback Serializer to resolve
            }
        } else {
            // For unnormal class
            return fallbackSerializer.encodeValue(object);
        }
    }
}
