package com.mongodb.client.model.expressions.codecs;

import com.mongodb.client.model.expressions.Expression;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

public class ExpressionCodecProvider implements CodecProvider {
    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if (Expression.class.isAssignableFrom(clazz)) {
            return (Codec<T>) new ExpressionCodec(registry);
        }
        return null;
    }
}
