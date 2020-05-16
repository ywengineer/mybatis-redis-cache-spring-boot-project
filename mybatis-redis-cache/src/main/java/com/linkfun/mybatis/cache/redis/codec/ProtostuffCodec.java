package com.linkfun.mybatis.cache.redis.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.lettuce.core.codec.RedisCodec;
import io.netty.util.concurrent.FastThreadLocal;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-16
 * Time: 20:25
 */
@Slf4j
public enum ProtostuffCodec implements RedisCodec<String, Object> {
    INSTANCE;

    private RedisCodec<String, Object> fallbackSerializer;
    // Setup ThreadLocal of Kryo instances
    private static final FastThreadLocal<LinkedBuffer> BUFFER_FAST_THREAD_LOCAL = new FastThreadLocal<LinkedBuffer>() {
        @Override
        protected LinkedBuffer initialValue() throws Exception {
            return LinkedBuffer.allocate(512);
        }
    };
    private final Schema<Wrapper> schema = RuntimeSchema.getSchema(Wrapper.class);

    ProtostuffCodec() {
        this.fallbackSerializer = KryoCodec.INSTANCE;
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
        // deser
        try {
            Wrapper wrapper = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(Strings.readAll(bytes), wrapper, schema);
            return wrapper.getValue();
        } catch (Exception e) {
            return this.fallbackSerializer.decodeValue(bytes);
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
     * @param value the value, may be {@literal null}.
     * @return The encoded value, never {@literal null}.
     */
    @Override
    public ByteBuffer encodeValue(Object value) {
        // Re-use (manage) this buffer to avoid allocating on every serialization
        LinkedBuffer buffer = BUFFER_FAST_THREAD_LOCAL.get();
        // ser
        try {
            return ByteBuffer.wrap(ProtostuffIOUtil.toByteArray(Wrapper.valueOf(value), schema, buffer));
        } catch (Throwable throwable) {
            return this.fallbackSerializer.encodeValue(value);
        } finally {
            buffer.clear();
        }
    }

    @Getter
    @Setter
    public static class Wrapper {
        private transient int non;
        private Object value;

        static Wrapper valueOf(Object value) {
            Wrapper wrapper = new Wrapper();
            wrapper.setValue(value);
            return wrapper;
        }
    }
}
