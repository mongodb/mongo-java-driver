/*
 * Copyright (c) 2008 - 2014 10gen, Inc. <http://10gen.com>
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

package org.bson.types;

/**
 * Represents the BSON Undefined type - since this type is deprecated, and can't effectively be writter to or read from the protocol,
 * this type is simply a placeholder.
 *
 * @see <a href="http://bsonspec.org/spec.html">BSON Spec</a>
 * @since 3.0
 */
public class Undefined {
}
