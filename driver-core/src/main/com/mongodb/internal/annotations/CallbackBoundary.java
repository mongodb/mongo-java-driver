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

package com.mongodb.internal.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a callback boundary: the outermost crossing where a {@code SingleResultCallback}
 * constructed outside the internal async machinery enters it. Exceptions thrown by the callback
 * itself must be contained here (via {@code errorHandlingCallback} or equivalent) so they cannot
 * unwind driver internals; conversely, code below a boundary must not swallow callback exceptions,
 * so that internal bugs fail fast rather than hanging silently.
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface CallbackBoundary {
    /**
     * Optional note on the containment mechanism or why this crossing exists.
     */
    String value() default "";
}
