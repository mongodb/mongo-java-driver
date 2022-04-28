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

package com.mongodb.client.model.changestream;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static java.lang.String.format;

/**
 * Change Stream fullDocumentBeforeChange configuration.
 *
 * <p>
 * Determines what to return for update operations when using a Change Stream. Defaults to {@link FullDocumentBeforeChange#DEFAULT}.
 * </p>
 *
 * @since 4.7
 * @mongodb.server.release 6.0
 */
public enum FullDocumentBeforeChange {
    /**
     * The default value
     */
    DEFAULT("default"),

    /**
     * Configures the change stream to not include the pre-image of the modified document.
     */
    OFF("off"),

    /**
     * Configures the change stream to return the pre-image of the modified document for replace, update, and delete change events if it
     * is available.
     */
    WHEN_AVAILABLE("whenAvailable"),

    /**
     * The same behavior as {@link #WHEN_AVAILABLE} except that an error is raised by the server if the pre-image is not available.
     */
    REQUIRED("required");


    private final String value;

    /**
     * The string value.
     *
     * @return the string value
     */
    public String getValue() {
        return value;
    }

    FullDocumentBeforeChange(final String value) {
        this.value = value;
    }

    /**
     * Returns the FullDocumentBeforeChange from the string value.
     *
     * @param value the string value.
     * @return the full document before change
     */
    public static FullDocumentBeforeChange fromString(final String value) {
        assertNotNull(value);
        for (FullDocumentBeforeChange fullDocumentBeforeChange : FullDocumentBeforeChange.values()) {
            if (value.equals(fullDocumentBeforeChange.value)) {
                return fullDocumentBeforeChange;
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid FullDocumentBeforeChange", value));
    }}
