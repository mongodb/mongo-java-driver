package com.mongodb.client.model.expressions;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

final class SingleValueExpression implements Expression {
    private final String name;
    private final Expression expr;

    public SingleValueExpression(String name, Expression expr) {
        this.name = name;
        this.expr = expr;
    }

    @Override
    public BsonValue toBsonValue(CodecRegistry codecRegistry) {
        return new BsonDocument(name, expr.toBsonValue(codecRegistry));
    }
}
