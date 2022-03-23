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
 * Signifies that the annotated program element is subject to incompatible changes by means of adding abstract methods.
 * This, in turn, means that implementing interfaces or extending classes annotated with {@code @}{@link Evolving} bears the risk
 * of doing extra work during upgrades.
 * Using such program elements is no different from using ordinary unannotated program elements.
 * Note that the presence of this annotation implies nothing about the quality or performance of the API in question.
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
@Documented
@Evolving
public @interface Evolving {
}
