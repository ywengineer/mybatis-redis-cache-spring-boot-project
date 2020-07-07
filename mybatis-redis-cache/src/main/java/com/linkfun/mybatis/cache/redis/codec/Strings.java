package com.linkfun.mybatis.cache.redis.codec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Description:
 * <p>
 * User: Mark.Yang
 * Email: ywengineer@gmail.com
 * Date: 2020-05-05
 * Time: 00:04
 */
public final class Strings {
    public static String decode(ByteBuffer buffer, Charset charset) {
        return new String(readAll(buffer), charset);
    }

    public static byte[] readAll(ByteBuffer buffer) {
        byte[] b = new byte[buffer.remaining()];
        buffer.get(b);
        return b;
    }

    public static String nullEmpty(String v) {
        return v == null ? "" : v;
    }
}
