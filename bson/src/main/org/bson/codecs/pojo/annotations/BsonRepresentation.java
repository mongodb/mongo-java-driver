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

package org.bson.codecs.pojo.annotations;

import org.bson.BsonType;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that specifies what type the property is stored as in the database.
 *
 * <p>For POJOs, requires the {@link org.bson.codecs.pojo.Conventions#ANNOTATION_CONVENTION}</p>
 * <p>For Java records, the annotation is only supported on the record component.</p>
 * <p>For Kotlin data classes, the annotation is only supported on the constructor parameter.</p>
 *
 * @since 4.2
 * @see org.bson.codecs.pojo.Conventions#ANNOTATION_CONVENTION
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
public @interface BsonRepresentation {
    /**
     * The type that the property is stored as in the database.
     *
     * @return the type that the property should be stored as.
     */
    BsonType value();
}
