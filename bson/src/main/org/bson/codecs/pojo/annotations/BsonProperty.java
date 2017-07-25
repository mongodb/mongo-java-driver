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

import org.bson.codecs.pojo.PropertyModel;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that configures a property.
 *
 * <p>Note: Requires the {@link org.bson.codecs.pojo.Conventions#ANNOTATION_CONVENTION}</p>
 *
 * @since 3.5
 * @see org.bson.codecs.pojo.Conventions#ANNOTATION_CONVENTION
 */
@Documented
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface BsonProperty {
    /**
     * The value to use for the mapped property when storing the property in the database.
     * @return the name to use for the property
     * @see PropertyModel#getWriteName()
     */
    String value() default "";

    /**
     * @return whether to include a discriminator when serializing nested Pojos.
     */
    boolean useDiscriminator() default false;
}
