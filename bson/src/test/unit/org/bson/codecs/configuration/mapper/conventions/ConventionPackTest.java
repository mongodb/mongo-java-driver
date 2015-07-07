/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.bson.codecs.configuration.mapper.conventions;

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.mapper.ClassModelCodecProvider;
import org.bson.codecs.configuration.mapper.Entity;
import org.bson.codecs.configuration.mapper.conventions.entities.SecureEntity;
import org.junit.Assert;
import org.junit.Test;

public class ConventionPackTest {

    @Test
    public void testCustomConventions() {
        final ClassModelCodecProvider codecProvider = ClassModelCodecProvider
                                                          .builder()
                                                          .setConventionPack(new CustomConventionPack())
                                                          .register(Entity.class)
                                                          .build();
        final CodecRegistry registry = CodecRegistries.fromProviders(codecProvider, new ValueCodecProvider());

        final Codec<Entity> codec = registry.get(Entity.class);
        final BsonDocument document = new BsonDocument();
        final BsonDocumentWriter writer = new BsonDocumentWriter(document);
        codec.encode(writer, new Entity(102L, 0, "Scrooge", "Ebenezer Scrooge"), EncoderContext.builder().build());
        Assert.assertEquals(document.getNumber("age").longValue(), 102L);
        Assert.assertEquals(document.getNumber("faves").intValue(), 0);
        Assert.assertEquals(document.getString("name").getValue(), "Scrooge");
        Assert.assertEquals(document.getString("full_name").getValue(), "Ebenezer Scrooge");
        Assert.assertFalse(document.containsKey("debug"));
    }

    @Test
    public void testDefaultConventions() {
        final ClassModelCodecProvider codecProvider = ClassModelCodecProvider
                                                          .builder()
                                                          .register(Entity.class)
                                                          .build();
        final CodecRegistry registry = CodecRegistries.fromProviders(codecProvider, new ValueCodecProvider());

        final Codec<Entity> codec = registry.get(Entity.class);
        final BsonDocument document = new BsonDocument();
        final BsonDocumentWriter writer = new BsonDocumentWriter(document);
        codec.encode(writer, new Entity(102L, 0, "Scrooge", "Ebenezer Scrooge"), EncoderContext.builder().build());
        Assert.assertEquals(document.getNumber("age").longValue(), 102L);
        Assert.assertEquals(document.getNumber("faves").intValue(), 0);
        Assert.assertEquals(document.getString("name").getValue(), "Scrooge");
        Assert.assertEquals(document.getString("fullName").getValue(), "Ebenezer Scrooge");
        Assert.assertFalse(document.containsKey("debug"));
    }

    @Test
    public void testTransformingConventions() {
        final ClassModelCodecProvider codecProvider = ClassModelCodecProvider
                                                          .builder()
                                                          .setConventionPack(new TransformingConventionPack())
                                                          .register(SecureEntity.class)
                                                          .build();
        final CodecRegistry registry = CodecRegistries.fromProviders(codecProvider, new ValueCodecProvider());

        final Codec<SecureEntity> codec = registry.get(SecureEntity.class);
        final BsonDocument document = new BsonDocument();
        final BsonDocumentWriter writer = new BsonDocumentWriter(document);
        final SecureEntity entity = new SecureEntity("Bob", "my voice is my passport");

        codec.encode(writer, entity, EncoderContext.builder().build());
        Assert.assertEquals(document.getString("name").getValue(), "Bob");
        Assert.assertEquals(document.getString("password").getValue(), "zl ibvpr vf zl cnffcbeg");

        Assert.assertEquals(entity, codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build()));
    }

}

