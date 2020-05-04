package com.linkfun.mybatis.cache.redis.codec;

import java.nio.ByteBuffer;

import io.lettuce.core.codec.RedisCodec;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-04
 * Time: 22:16
 */
public enum ByteBufferCodec implements RedisCodec<ByteBuffer, ByteBuffer> {

    INSTANCE;

    @Override
    public ByteBuffer decodeKey(ByteBuffer bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(bytes.remaining());
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    @Override
    public ByteBuffer decodeValue(ByteBuffer bytes) {
        return decodeKey(bytes);
    }

    @Override
    public ByteBuffer encodeKey(ByteBuffer key) {
        return key.duplicate();
    }

    @Override
    public ByteBuffer encodeValue(ByteBuffer value) {
        return value.duplicate();
    }
}