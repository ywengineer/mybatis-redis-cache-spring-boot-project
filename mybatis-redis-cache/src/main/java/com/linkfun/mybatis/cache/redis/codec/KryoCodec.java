
package com.linkfun.mybatis.cache.redis.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.google.common.collect.Sets;
import io.lettuce.core.codec.RedisCodec;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.commons.lang3.tuple.Triple;

public enum KryoCodec implements RedisCodec<String, Object> {
    // Enum singleton, which is preferred approach since Java 1.5
    INSTANCE;

    private Set<Class<?>> unNormalClassSet;
    private Set<Integer> unNormalBytesHashCodeSet;

    private RedisCodec<String, Object> fallbackSerializer;
    // Setup ThreadLocal of Kryo instances
    private static final FastThreadLocal<Triple<Kryo, Input, Output>> kryos = new FastThreadLocal<Triple<Kryo, Input, Output>>() {
        @Override
        protected Triple<Kryo, Input, Output> initialValue() throws Exception {
            final Kryo kryo = new Kryo();
            kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
//            kryo.getFieldSerializerConfig().setCachedFieldNameStrategy(FieldSerializer.CachedFieldNameStrategy.EXTENDED);
            return Triple.of(kryo, new Input(), new Output(200, -1));
        }
    };

    KryoCodec() {
        unNormalClassSet = Sets.newConcurrentHashSet();
        unNormalBytesHashCodeSet = Sets.newConcurrentHashSet();
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
        if (!unNormalBytesHashCodeSet.contains(hashCode)) {
            /**
             * In the following cases: 1. This bytes occurs for the first time. 2. This bytes have occurred and can be
             * resolved by default kryo serializer
             */
            try {
                final Triple<Kryo, Input, Output> triple = kryos.get();
                triple.getMiddle().setBuffer(b);
                return triple.getLeft().readClassAndObject(triple.getMiddle());
            } catch (Exception e) {
                // For unnormal bytes occurred for the first time, exception will be thrown
                unNormalBytesHashCodeSet.add(hashCode);
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
        if (!unNormalClassSet.contains(object.getClass())) {
            /**
             * In the following cases: 1. This class occurs for the first time. 2. This class have occurred and can be
             * resolved by default kryo serializer
             */
            try {
                final Triple<Kryo, Input, Output> triple = kryos.get();
                triple.getRight().clear();
                triple.getLeft().writeClassAndObject(triple.getRight(), object);
                return ByteBuffer.wrap(triple.getRight().toBytes());
            } catch (Exception e) {
                // For unnormal class occurred for the first time, exception will be thrown
                unNormalClassSet.add(object.getClass());
                return fallbackSerializer.encodeValue(object);// use fallback Serializer to resolve
            }
        } else {
            // For unnormal class
            return fallbackSerializer.encodeValue(object);
        }
    }
}
