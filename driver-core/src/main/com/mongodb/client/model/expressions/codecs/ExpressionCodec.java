package com.mongodb.client.model.expressions.codecs;

import com.mongodb.client.model.expressions.Expression;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

public final class ExpressionCodec implements Codec<Expression> {
    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(new BsonValueCodecProvider());

    private final CodecRegistry codecRegistry;

    /**
     * Creates a new instance initialised with the default codec registry.
     *
     */
    public ExpressionCodec() {
        this(DEFAULT_REGISTRY);
    }

    /**
     * Creates a new instance initialised with the given codec registry.
     *
     * @param codecRegistry the {@code CodecRegistry} to use to look up the codecs for encoding and decoding to/from BSON
     */
    public ExpressionCodec(final CodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
    }

    @Override
    public Expression decode(BsonReader reader, DecoderContext decoderContext) {
        throw new UnsupportedOperationException("Decoding to an expression is not supported");
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void encode(BsonWriter writer, Expression value, EncoderContext encoderContext) {
        BsonValue bsonValue = value.toBsonValue(codecRegistry);
        Codec codec = codecRegistry.get(bsonValue.getClass());
        codec.encode(writer, bsonValue, encoderContext);
    }

    @Override
    public Class<Expression> getEncoderClass() {
        return Expression.class;
    }
}
