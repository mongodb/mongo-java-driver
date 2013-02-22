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

package org.mongodb.json;

/**
 * An enumeration of the supported output modes of {@code JsonWriter}.  The first three correspond to the syntax documented
 * <a href="http://www.mongodb.org/display/DOCS/Mongo+Extended+JSON">here</a>.
 *
 * @see JsonWriter
 * @since 3.0.0
 */
public enum JsonOutputMode {

    /**
     * Strict mode produces output conforming to the <a href="http://www.json.org">JSON RFC spec</a>.
     */
    Strict,

    /**
     * JavaScript mode produces output that can be processed by most Javascript interpreters.
     */
    JavaScript,

    /**
     * TenGen mode produces output that the MongoDB shell understands. This is basically an enhanced Javascript format.
     */
    TenGen,

    /**
     * While not formally documented, this output mode will attempt to produce output that corresponds to what
     * the MongoDB shell actually produces when showing query results.
     */
    Shell
}
