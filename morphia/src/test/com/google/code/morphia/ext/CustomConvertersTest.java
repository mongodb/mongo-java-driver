/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.ext;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Converters;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.converters.IntegerConverter;
import com.google.code.morphia.converters.SimpleValueConverter;
import com.google.code.morphia.converters.TypeConverter;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Scott Hernandez
 */
public class CustomConvertersTest extends TestBase {

    @SuppressWarnings("rawtypes")
    static class CharacterToByteConverter extends TypeConverter implements SimpleValueConverter {
        public CharacterToByteConverter() {
            super(Character.class, char.class);
        }

        @Override
        public Object decode(final Class targetClass, final Object fromDBObject, final MappedField optionalExtraInfo)
                throws MappingException {
            if (fromDBObject == null) {
                return null;
            }
            final IntegerConverter intConv = new IntegerConverter();
            final Integer i = (Integer) intConv.decode(targetClass, fromDBObject, optionalExtraInfo);
            return new Character((char) i.intValue());
        }

        @Override
        public Object encode(final Object value, final MappedField optionalExtraInfo) {
            final Character c = (Character) value;
            return (int) c.charValue();
        }
    }

    @Converters(CharacterToByteConverter.class)
    static class CharEntity {
        @Id
        ObjectId id = new ObjectId();
        final Character c = 'a';
    }

    @Test
    public void testIt() {
        morphia.map(CharEntity.class);

        ds.save(new CharEntity());
        final CharEntity ce = ds.find(CharEntity.class).get();
        Assert.assertNotNull(ce.c);
        Assert.assertEquals(ce.c.charValue(), 'a');

        final BasicDBObject dbObj = (BasicDBObject) ds.getCollection(CharEntity.class).findOne();
        Assert.assertTrue(dbObj.getInt("c") == (int) 'a');
    }

    /**
     * This test shows an issue with an <code>@Embedded</code> class A inheriting from an <code>@Embedded</code> class B
     * that both have a Converter assigned (A has AConverter, B has BConverter). <p> When an object (here MyEntity)
     * has a
     * property/field of type A and is deserialized, the deserialization fails with a
     * "com.google.code.morphia.mapping.MappingException: No usable constructor for A" . </p>
     */

    @Entity(noClassnameStored = true)
    private static class MyEntity {

        @Id
        private Long id;
        @Embedded
        private A a;

        public MyEntity() {
        }

        public MyEntity(final Long id, final A a) {
            this.id = id;
            this.a = a;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((a == null) ? 0 : a.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final MyEntity other = (MyEntity) obj;
            if (id == null) {
                if (other.id != null) {
                    return false;
                }
            }
            else if (!id.equals(other.id)) {
                return false;
            }
            if (a == null) {
                if (other.a != null) {
                    return false;
                }
            }
            else if (!a.equals(other.a)) {
                return false;
            }
            return true;
        }

    }

    @Converters(B.BConverter.class)
    @Embedded
    private static class B {

        static class BConverter extends TypeConverter implements
                SimpleValueConverter {

            public BConverter() {
                this(B.class);
            }

            public BConverter(final Class<? extends B> clazz) {
                super(clazz);
            }

            @SuppressWarnings({"rawtypes"})
            @Override
            public B decode(final Class targetClass, final Object fromDBObject,
                            final MappedField optionalExtraInfo)
                    throws MappingException {
                if (fromDBObject == null) {
                    return null;
                }
                final Long source = (Long) fromDBObject;
                return create(source);
            }

            protected B create(final Long source) {
                return new B(source);
            }

            @Override
            public Long encode(final Object value,
                               final MappedField optionalExtraInfo) {
                if (value == null) {
                    return null;
                }
                final B source = (B) value;
                return source.value;
            }

        }

        private final long value;

        public B(final long value) {
            super();
            this.value = value;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (value ^ (value >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final B other = (B) obj;
            if (value != other.value) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " [value=" + value + "]";
        }

    }

    @Converters(A.AConverter.class)
    @Embedded
    private static class A extends B {

        static final class AConverter extends B.BConverter {

            public AConverter() {
                super(A.class);
            }

            @Override
            protected A create(final Long source) {
                return new A(source);
            }

        }

        public A(final long value) {
            super(value);
        }
    }

    @Before
    public void setup() {
        morphia.map(MyEntity.class);
        morphia.map(B.class);
        morphia.map(A.class);
    }

    /**
     * This test is green when {@link MyEntity#a} is annotated with <code>@Property</code>, as in this case the field is
     * not serialized at all. However, the bson encoder would fail to encode the object of type A
     */
    @Test
    @Ignore
    public void testDBObjectSerialization() {
        final MyEntity entity = new MyEntity(1l, new A(2));
        final DBObject dbObject = morphia.toDBObject(entity);
        assertEquals(BasicDBObjectBuilder.start("_id", 1l).add("a", 2l).get(),
                     dbObject);
        // fails with a
        // com.google.code.morphia.mapping.MappingException: No usable
        // constructor
        // for InheritanceTest$A
        final MyEntity actual = morphia.fromDBObject(MyEntity.class, dbObject);
        assertEquals(entity, actual);
    }

}

