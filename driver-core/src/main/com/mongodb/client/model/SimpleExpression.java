/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

class SimpleExpression<TExpression> implements Bson {
    private final String name;
    private final TExpression expression;

    public SimpleExpression(final String name, final TExpression expression) {
        this.name = name;
        this.expression = expression;
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
        BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        writer.writeStartDocument();
        writer.writeName(name);
        BuildersHelper.encodeValue(writer, expression, codecRegistry);
        writer.writeEndDocument();

        return writer.getDocument();
    }

    @Override
    public String toString() {
        return "Expression{"
                       + "name='" + name + '\''
                       + ", expression=" + expression
                       + '}';
    }
}
