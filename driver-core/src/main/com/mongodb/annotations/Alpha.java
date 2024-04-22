/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2010 The Guava Authors
 * Copyright 2011 The Guava Authors
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
package com.mongodb.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Signifies that a public API (public class, method or field) is in the early stages
 * of development, subject to incompatible changes, or even removal, in a future release
 * and may lack some intended features. An API bearing this annotation may be unstable,
 * have potential performance implications as development progresses, and is exempt from
 * any compatibility guarantees made by its containing library.
 *
 * <p>It is inadvisable for <i>applications</i> to use Alpha APIs in production environments or
 * for <i>libraries</i> (which get included on users' CLASSPATHs, outside the library developers'
 * control) to depend on these APIs. Alpha APIs are intended for <b>experimental purposes</b> only.</p>
 */
@Retention(RetentionPolicy.CLASS)
@Target({
        ElementType.ANNOTATION_TYPE,
        ElementType.CONSTRUCTOR,
        ElementType.FIELD,
        ElementType.METHOD,
        ElementType.PACKAGE,
        ElementType.TYPE })
@Documented
@Alpha(Alpha.Reason.CLIENT)
public @interface Alpha {
    /**
     * @return The reason an API element is marked with {@link Alpha}.
     */
    Alpha.Reason[] value();

    /**
     * @see Alpha#value()
     */
    enum Reason {
        /**
         * Indicates that the driver API is either experimental or in development.
         * Use in production environments is inadvisable due to potential API changes and possible instability.
         */
        CLIENT,
    }
}
