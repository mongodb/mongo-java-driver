/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.changestream;

import com.mongodb.MongoNamespace;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.codecs.pojo.PropertyModelBuilder;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@SuppressWarnings({"unchecked", "rawtypes"})
final class ChangeStreamDocumentCodec<TResult> implements Codec<ChangeStreamDocument<TResult>> {

    private static final OperationTypeCodec OPERATION_TYPE_CODEC = new OperationTypeCodec();

    private final Codec<ChangeStreamDocument<TResult>> codec;

    ChangeStreamDocumentCodec(final Class<TResult> fullDocumentClass, final CodecRegistry codecRegistry) {

        ClassModelBuilder<ChangeStreamDocument> classModelBuilder = ClassModel.builder(ChangeStreamDocument.class);
        ((PropertyModelBuilder<TResult>) classModelBuilder.getProperty("fullDocument")).codec(codecRegistry.get(fullDocumentClass));
        ((PropertyModelBuilder<OperationType>) classModelBuilder.getProperty("operationType")).codec(OPERATION_TYPE_CODEC);
        ClassModel<ChangeStreamDocument> changeStreamDocumentClassModel = classModelBuilder.build();

        PojoCodecProvider provider = PojoCodecProvider.builder()
                .register(MongoNamespace.class)
                .register(UpdateDescription.class)
                .register(changeStreamDocumentClassModel)
                .build();

        CodecRegistry registry = fromRegistries(fromProviders(provider, new BsonValueCodecProvider()), codecRegistry);
        this.codec = (Codec<ChangeStreamDocument<TResult>>) (Codec<? extends ChangeStreamDocument>)
                registry.get(ChangeStreamDocument.class);
    }

    @Override
    public ChangeStreamDocument<TResult> decode(final BsonReader reader, final DecoderContext decoderContext) {
        return codec.decode(reader, decoderContext);
    }

    @Override
    public void encode(final BsonWriter writer, final ChangeStreamDocument<TResult> value, final EncoderContext encoderContext) {
        codec.encode(writer, value, encoderContext);
    }

    @Override
    public Class<ChangeStreamDocument<TResult>> getEncoderClass() {
        return (Class<ChangeStreamDocument<TResult>>) (Class<? extends ChangeStreamDocument>) ChangeStreamDocument.class;
    }
}
