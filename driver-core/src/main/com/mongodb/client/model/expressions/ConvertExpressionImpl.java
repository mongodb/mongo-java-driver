package com.mongodb.client.model.expressions;

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

final class ConvertExpressionImpl implements ConvertExpression {
    private final Expression input;
    private final Expression to;
    private final Expression onError;
    private final Expression onNull;

    ConvertExpressionImpl(Expression input, Expression to, @Nullable Expression onError, @Nullable Expression onNull) {
        this.input = input;
        this.to = to;
        this.onError = onError;
        this.onNull = onNull;
    }

    @Override
    public ConvertExpression onError(final Expression value) {
        return new ConvertExpressionImpl(input, to, value, onNull);
    }

    @Override
    public ConvertExpression onNull(final Expression value) {
        return new ConvertExpressionImpl(input, to, onError, value);
    }

    @Override
    public BsonValue toBsonValue(CodecRegistry codecRegistry) {
        BsonDocument arguments = new BsonDocument("input", input.toBsonValue(codecRegistry))
                .append("to", to.toBsonValue(codecRegistry));
        if (onError != null) {
            arguments.append("onError", onError.toBsonValue(codecRegistry));
        }
        if (onNull != null) {
            arguments.append("onNull", onNull.toBsonValue(codecRegistry));
        }
        return new BsonDocument("$convert", arguments);
    }
}
