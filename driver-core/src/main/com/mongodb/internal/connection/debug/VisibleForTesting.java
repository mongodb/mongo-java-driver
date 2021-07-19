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
package com.mongodb.internal.connection.debug;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Denotes that the annotated program element is made more accessible than otherwise necessary for the purpose of testing.
 * The annotated program element must be used as if it had the {@linkplain #otherwise() intended} access modifier for any purpose other
 * than testing.
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.TYPE, ElementType.CONSTRUCTOR, ElementType.METHOD, ElementType.FIELD})
@interface VisibleForTesting {
    /**
     * The intended {@link AccessModifier}.
     */
    AccessModifier otherwise();

    /**
     * A subset of <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-6.html#jls-6.6.1">access modifiers</a>
     * that includes only values relevant to be used as the {@linkplain #otherwise() intended} access modifier.
     */
    enum AccessModifier {
        PRIVATE,
        PACKAGE,
        PROTECTED
    }
}
