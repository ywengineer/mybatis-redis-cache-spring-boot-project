package com.linkfun.mybatis.cache.redis.codec.protostuff.delegate;

import java.io.IOException;
import java.sql.Date;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Pipe;
import io.protostuff.WireFormat;
import io.protostuff.runtime.Delegate;

/**
 * Custom {@link java.sql.Date} delegate
 */
public class SqlDateDelegate implements Delegate<Date> {
    @Override
    public WireFormat.FieldType getFieldType() {
        return WireFormat.FieldType.FIXED64;
    }

    @Override
    public Date readFrom(Input input) throws IOException {
        return new Date(input.readFixed64());
    }

    @Override
    public void writeTo(Output output, int number, Date value, boolean repeated) throws IOException {
        output.writeFixed64(number, value.getTime(), repeated);
    }

    @Override
    public void transfer(Pipe pipe, Input input, Output output, int number, boolean repeated) throws IOException {
        output.writeFixed64(number, input.readFixed64(), repeated);
    }

    @Override
    public Class<?> typeClass() {
        return Date.class;
    }
}
