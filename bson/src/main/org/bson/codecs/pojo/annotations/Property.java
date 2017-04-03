/*
 * Copyright 2017 MongoDB, Inc.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that configures a field.
 *
 * <p>Note: Requires the {@link org.bson.codecs.pojo.Conventions#ANNOTATION_CONVENTION}</p>
 *
 * @since 3.5
 * @see org.bson.codecs.pojo.Conventions#ANNOTATION_CONVENTION
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Property {
    /**
     * The name to use for the mapped field when storing the field in the database.
     * @return the name to use for the field
     * @see org.bson.codecs.pojo.FieldModel#getDocumentFieldName()
     */
    String name() default "";

    /**
     * @return whether to include a discriminator when serializing nested Pojos.
     */
    boolean useDiscriminator() default false;
}
