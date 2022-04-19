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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that configures a property to be used as storage for any extra BSON elements that are not already mapped to other
 * properties. All extra elements will be encoded from the BSON document into the annotated property, and encoded from the annotated
 * property into the BSON document.
 *
 * <p>Can only be used on a single field in a POJO. Field must be a {@code Map<String, Object>} instance eg. {@code Document}.
 * <p>Note: Requires the {@link org.bson.codecs.pojo.Conventions#ANNOTATION_CONVENTION}</p>
 *
 * @since 4.7
 * @see org.bson.codecs.pojo.Conventions#ANNOTATION_CONVENTION
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface BsonExtraElements {
}
