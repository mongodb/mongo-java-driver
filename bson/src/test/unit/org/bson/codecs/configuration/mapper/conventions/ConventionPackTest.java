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
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.mapper.ClassModel;
import org.bson.codecs.configuration.mapper.ClassModelCodecProvider;
import org.bson.codecs.configuration.mapper.Entity;
import org.bson.codecs.configuration.mapper.FieldModel;
import org.bson.codecs.configuration.mapper.Weights;
import org.junit.Assert;
import org.junit.Test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
@interface Secure {
}

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
        codec.encode(writer, new Entity(102L, 0, "Scrooge", "Ebenzer Scrooge"), EncoderContext.builder().build());
        Assert.assertEquals(document.getNumber("age").longValue(), 102L);
        Assert.assertEquals(document.getNumber("faves").intValue(), 0);
        Assert.assertEquals(document.getString("name").getValue(), "Scrooge");
        Assert.assertEquals(document.getString("full_name").getValue(), "Ebenzer Scrooge");
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

    public static class SecureEntity {
        private String name;
        @Secure
        private String password;

        public SecureEntity() {
        }

        public SecureEntity(final String name, final String password) {
            this.name = name;
            this.password = password;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(final String password) {
            this.password = password;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final SecureEntity that = (SecureEntity) o;

            if (name != null ? !name.equals(that.name) : that.name != null) {
                return false;
            }
            return !(password != null ? !password.equals(that.password) : that.password != null);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (password != null ? password.hashCode() : 0);
            return result;
        }
    }

}

class TransformingConventionPack extends DefaultConventionPack {
    public TransformingConventionPack() {
        addConvention(new Rot13Convention());
    }

}

class Rot13Convention implements Convention {
    @Override
    public void apply(final ClassModel classModel) {
        for (final FieldModel fieldModel : classModel.getFields()) {
            if (fieldModel.hasAnnotation(Secure.class)) {
                classModel.addField(new Rot13FieldModel(fieldModel));
                fieldModel.setIncluded(Weights.USER_ATTRIBUTE, false);
            }
        }
    }

    @Override
    public String getPhase() {
        return ConventionPack.FIELD_MAPPING;
    }

}

class Rot13FieldModel extends FieldModel {
    private final FieldModel original;

    public Rot13FieldModel(final FieldModel fieldModel) {
        super(String.class, fieldModel);
        original = fieldModel;
    }

    @Override
    public void store(final BsonWriter writer, final Object entity, final EncoderContext encoderContext) {
        final Object value = get(entity);
        if (value != null) {
            writer.writeName(getName());
            original.getCodec().encode(writer, value, encoderContext);
        }
        
    }

    @Override
    public void set(final Object entity, final Object value) {
        original.set(entity, rot13((String) value));
    }

    @Override
    public Object get(final Object entity) {
        return rot13((String) original.get(entity));
    }

    @Override
    public Field getRawField() {
        return original.getRawField();
    }

    private String rot13(final String value) {
        if (value == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (char c : value.toCharArray()) {
            if (c >= 'a' && c <= 'm') {
                c += 13;
            } else if (c >= 'n' && c <= 'z') {
                c -= 13;
            } else if (c >= 'A' && c <= 'M') {
                c += 13;
            } else if (c >= 'N' && c <= 'Z') {
                c -= 13;
            }
            sb.append(c);
        }

        return sb.toString();
    }
}

class CustomConventionPack extends DefaultConventionPack {
    public CustomConventionPack() {
        addConvention(new SnakeCaseConvention());
    }
}

class SnakeCaseConvention implements Convention {
    @Override
    public void apply(final ClassModel model) {
        for (final FieldModel fieldModel : model.getFields()) {
            fieldModel.setName(Weights.USER_CONVENTION, snake(fieldModel.getFieldName()));
        }
    }

    private String snake(final String name) {
        return name.replaceAll("([A-Z])", "_$1").toLowerCase();
    }

    @Override
    public String getPhase() {
        return ConventionPack.FIELD_MAPPING;
    }
}
