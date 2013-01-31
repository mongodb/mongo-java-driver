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

package com.google.code.morphia.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotation which helps migrate schemas by loading one of several possible properties in the document into fields
 * or methods.  This is typically used when a field is renamed, allowing the field to be populated by both its current
 * name and any prior names.</p>
 * <p/>
 * <ul> <li>When placed on a field, the additional names (document field) will be checked when this field is loaded.  If
 * the document contains data for more than one of the names, an exception will be thrown. <li>When placed on a
 * parameter to a method that takes a single parameter, the method will be called with the data value.  As with fields,
 * any ambiguity in the data (multiple properties that would cause the method to be called) will produce an exception.
 * However, {@code @AlsoLoad} on a method parameter *can* be used to override field names and "steal" the value that
 * would otherwise have been set on a field.  This can be useful when changing the type of a field.</li> </ul>
 * <p/>
 * (orig @author Jeff Schnitzer <jeff@infohazard.org> for Objectify)
 *
 * @author Scott Hernandez
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.PARAMETER })
public @interface AlsoLoad {
    String[] value();
}