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
 * Marks the pre-reshape version of a sync method or type that has been refactored (reshaped)
 * to enable async generation. The defunct copy is kept temporarily as in-tree reference for
 * reviewing the reshape's equivalence and is to be deleted in the endgame cleanup. Where the
 * replacement keeps the same signature, the defunct copy is renamed with a {@code Defunct}
 * suffix. Elements carrying this annotation are never generation targets.
 */
@Documented
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface DeadSync {
    /**
     * Optional note on what replaced this element.
     */
    String value() default "";
}
