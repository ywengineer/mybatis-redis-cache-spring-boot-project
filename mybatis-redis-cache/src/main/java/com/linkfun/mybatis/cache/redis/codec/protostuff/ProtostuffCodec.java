package com.linkfun.mybatis.cache.redis.codec.protostuff;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.linkfun.mybatis.cache.redis.codec.KryoCodec;
import com.linkfun.mybatis.cache.redis.codec.Strings;
import com.linkfun.mybatis.cache.redis.codec.protostuff.utils.WrapperUtils;
import io.lettuce.core.codec.RedisCodec;
import io.netty.util.concurrent.FastThreadLocal;
import io.protostuff.GraphIOUtil;
import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
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
        try {
            int classNameLength = bytes.getInt();
            int bytesLength = bytes.getInt();

            if (classNameLength < 0 || bytesLength < 0) {
                return null;
            }

            byte[] classNameBytes = new byte[classNameLength];
            bytes.get(classNameBytes, 0, classNameLength);

            byte[] payload = new byte[bytesLength];
            bytes.get(payload, 0, bytesLength);

            String className = new String(classNameBytes);
            Class clazz = Class.forName(className);

            Object result;
            if (WrapperUtils.needWrapper(clazz)) {
                Schema<Wrapper> schema = RuntimeSchema.getSchema(Wrapper.class);
                Wrapper wrapper = schema.newMessage();
                GraphIOUtil.mergeFrom(payload, wrapper, schema);
                result = wrapper.getData();
            } else {
                Schema schema = RuntimeSchema.getSchema(clazz);
                result = schema.newMessage();
                GraphIOUtil.mergeFrom(payload, result, schema);
            }
            return result;
        } catch (ClassNotFoundException e) {
            log.error("failed to decode data from redis with protostuff codec.", e);
        }
        return null;
    }

    /**
     * Encode the key for output to redis.
     *
     * @param key the key, may be {@literal null}.
     * @return The encoded key, never {@literal null}.
     */
    @Override
    public ByteBuffer encodeKey(String key) {
        return ByteBuffer.wrap(Strings.nullEmpty(key).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Encode the value for output to redis.
     *
     * @param obj the value, may be {@literal null}.
     * @return The encoded value, never {@literal null}.
     */
    @Override
    public ByteBuffer encodeValue(Object obj) {
        byte[] bytes;
        byte[] classNameBytes;
        // Re-use (manage) this buffer to avoid allocating on every serialization
        LinkedBuffer buffer = BUFFER_FAST_THREAD_LOCAL.get();
        try {
            if (obj == null || WrapperUtils.needWrapper(obj)) {
                Schema<Wrapper> schema = RuntimeSchema.getSchema(Wrapper.class);
                Wrapper wrapper = new Wrapper(obj);
                bytes = GraphIOUtil.toByteArray(wrapper, schema, buffer);
                classNameBytes = Wrapper.class.getName().getBytes();
            } else {
                Schema schema = RuntimeSchema.getSchema(obj.getClass());
                bytes = GraphIOUtil.toByteArray(obj, schema, buffer);
                classNameBytes = obj.getClass().getName().getBytes();
            }
            //
            final ByteBuffer r = ByteBuffer.allocate(classNameBytes.length + bytes.length + 8);
            r.putInt(classNameBytes.length);
            r.putInt(bytes.length);
            r.put(classNameBytes);
            r.put(bytes);
            return r;
        } catch (Throwable throwable) {
            return this.fallbackSerializer.encodeValue(obj);
        } finally {
            buffer.clear();
        }
    }
}
