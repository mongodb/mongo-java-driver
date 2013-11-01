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

import org.bson.BSONReaderSettings;
import org.mongodb.annotations.Immutable;

/**
 * Settings to control the behavior of a {@code JSONReader} instance.
 *
 * @see JSONWriter
 * @since 3.0.0
 */
@Immutable
public class JSONReaderSettings extends BSONReaderSettings {
    private final JSONMode inputMode;

    /**
     * Creates a new instance with default values for all properties.
     */
    public JSONReaderSettings() {
        this(JSONMode.STRICT);
    }

    /**
     * Creates a new instance with the given output inputMode and default values for all other properties.
     *
     * @param mode the input mode
     */
    public JSONReaderSettings(final JSONMode mode) {
        this.inputMode = mode;
    }
}
