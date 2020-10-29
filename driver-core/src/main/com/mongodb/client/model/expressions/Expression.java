package com.mongodb.client.model.expressions;

import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

public interface Expression {
    BsonValue toBsonValue(CodecRegistry codecRegistry);
}
