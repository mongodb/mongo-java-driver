/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

/**
 *
 */
package com.google.code.morphia.callbacks;

import com.google.code.morphia.AbstractEntityInterceptor;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PrePersist;
import com.google.code.morphia.callbacks.SimpleValidationViaInterceptorTest.NonNullValidation
       .NonNullValidationException;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.List;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class SimpleValidationViaInterceptorTest extends TestBase {

    static class E {
        @Id
        private final ObjectId id = new ObjectId();

        @NonNull
        private Date lastModified;

        @PrePersist
        void entityCallback() {
            lastModified = new Date();
        }
    }

    static class E2 {
        @Id
        private final ObjectId id = new ObjectId();

        @NonNull
        private String mustFailValidation;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    static @interface NonNull {
    }

    public static class NonNullValidation extends AbstractEntityInterceptor {
        public void prePersist(final Object ent, final DBObject dbObj, final Mapper mapr) {
            final MappedClass mc = mapr.getMappedClass(ent);
            final List<MappedField> fieldsToTest = mc.getFieldsAnnotatedWith(NonNull.class);
            for (final MappedField mf : fieldsToTest) {
                if (mf.getFieldValue(ent) == null) {
                    throw new NonNullValidationException(mf);
                }
            }
        }

        static class NonNullValidationException extends RuntimeException {

            private static final long serialVersionUID = 8441727716383001380L;

            public NonNullValidationException(final MappedField mf) {
                super("NonNull field is null " + mf.getFullName());
            }

        }
    }

    static {
        MappedField.interestingAnnotations.add(NonNull.class);
    }

    @Test
    public void testGlobalEntityInterceptorWorksAfterEntityCallback() {

        morphia.getMapper().addInterceptor(new NonNullValidation());
        morphia.map(E.class);
        morphia.map(E2.class);

        ds.save(new E());
        try {
            ds.save(new E2());
            Assert.fail();
        } catch (NonNullValidationException e) {
            // expected
        }

    }
}
