package com.mongodb.client.model.expressions;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;
import java.util.stream.Collectors;

final class ArrayValueExpression implements Expression {
    private final String name;
    private final List<Expression> expressions;

    public ArrayValueExpression(String name, List<Expression> expressions) {
        this.name = name;
        this.expressions = expressions;
    }


    @Override
    public BsonValue toBsonValue(CodecRegistry codecRegistry) {
        return new BsonDocument(name, new BsonArray(expressions.stream()
                .map(expression -> expression.toBsonValue(codecRegistry)).collect(Collectors.toList())));
    }
}
