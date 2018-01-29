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

package com.mongodb.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>The class to which this annotation is applied is immutable.  This means that its state cannot be seen to change by callers, which
 * implies that</p>
 * <ul>
 *     <li> all public fields are final, </li>
 *     <li> all public final reference fields refer to other immutable objects, and </li>
 *     <li> constructors and methods do not publish references to any internal state which is potentially mutable by the
 *          implementation. </li>
 * </ul>
 * <p>Immutable objects may still have internal mutable state for purposes of performance optimization; some state
 * variables may be lazily computed, so long as they are computed from immutable state and that callers cannot tell the difference. </p>
 *
 * <p>Immutable objects are inherently thread-safe; they may be passed between threads or published without synchronization.</p>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Immutable {
}
