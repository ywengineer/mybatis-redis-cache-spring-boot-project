package com.linkfun.mybatis.cache.redis.codec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import io.lettuce.core.codec.RedisCodec;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-04
 * Time: 22:39
 */
public class StringCodec implements RedisCodec<String, String> {

    private final Charset charset;

    public static final StringCodec US_ASCII = new StringCodec(StandardCharsets.US_ASCII);

    public static final StringCodec ISO_8859_1 = new StringCodec(StandardCharsets.ISO_8859_1);

    public static final StringCodec UTF_8 = new StringCodec(StandardCharsets.UTF_8);

    public StringCodec() {
        this(StandardCharsets.UTF_8);
    }

    /**
     * Creates a new {@link StringCodec} using the given {@link Charset} to encode and decode strings.
     *
     * @param charset must not be {@literal null}.
     */
    public StringCodec(Charset charset) {
        Objects.requireNonNull(charset, "Charset must not be null!");
        this.charset = charset;
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
    public String decodeValue(ByteBuffer bytes) {
        return Strings.decode(bytes, charset);
    }

    /**
     * Encode the key for output to redis.
     *
     * @param key the key, may be {@literal null}.
     * @return The encoded key, never {@literal null}.
     */
    @Override
    public ByteBuffer encodeKey(String key) {
        return ByteBuffer.wrap(Strings.nullEmpty(key).getBytes(this.charset));
    }

    /**
     * Encode the value for output to redis.
     *
     * @param value the value, may be {@literal null}.
     * @return The encoded value, never {@literal null}.
     */
    @Override
    public ByteBuffer encodeValue(String value) {
        return ByteBuffer.wrap(Strings.nullEmpty(value).getBytes(this.charset));
    }

}
