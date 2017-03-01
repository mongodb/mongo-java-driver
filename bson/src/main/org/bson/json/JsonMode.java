/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.bson.json;

/**
 * An enumeration of the supported output modes of {@code JSONWriter}.
 *
 * @see JsonWriter
 * @since 3.0
 */
public enum JsonMode {

    /**
     * Strict mode representations of BSON types conform to the <a href="http://www.json.org">JSON RFC spec</a>.
     *
     * @deprecated  The format generated with this mode is no longer considered standard for MongoDB tools.
     */
    @Deprecated
    STRICT,

    /**
     * While not formally documented, this output mode will attempt to produce output that corresponds to what the MongoDB shell actually
     * produces when showing query results.
     */
    SHELL,

    /**
     * Standard extended JSON representation.
     *
     * @since 3.5
     * @see <a href="https://github.com/mongodb/specifications/blob/master/source/extended-json.rst">Extended JSON Specification</a>
     */
    EXTENDED
}
