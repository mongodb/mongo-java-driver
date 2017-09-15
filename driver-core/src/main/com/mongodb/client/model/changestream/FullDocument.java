/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model.changestream;

import static java.lang.String.format;

/**
 *
 * Change Stream fullDocument configuration.
 *
 * <p>Determines what to return for update operations when using a Change Stream. Defaults to {@link FullDocument#DEFAULT}.
 * When set to {@link FullDocument#UPDATE_LOOKUP}, the change stream for partial updates will include both a delta describing the
 * changes to the document as well as a copy of the entire document that was changed from <em>some time</em> after the change occurred.</p>
 *
 * @since 3.6
 * @mongodb.server.release 3.6
 */
public enum FullDocument {

    /**
     * Default
     *
     * <p>Returns the servers default value in the {@code fullDocument} field.</p>
     */
    DEFAULT("default"),

    /**
     * Lookup
     *
     * <p>The change stream for partial updates will include both a delta describing the changes to the document as well as a copy of the
     * entire document that was changed from <em>some time</em> after the change occurred.</p>
     */
    UPDATE_LOOKUP("updateLookup");

    private final String value;
    FullDocument(final String caseFirst) {
        this.value = caseFirst;
    }

    /**
     * @return the String representation of the collation case first value
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the ChangeStreamFullDocument from the string value.
     *
     * @param changeStreamFullDocument the string value.
     * @return the read concern
     */
    public static FullDocument fromString(final String changeStreamFullDocument) {
        if (changeStreamFullDocument != null) {
            for (FullDocument fullDocument : FullDocument.values()) {
                if (changeStreamFullDocument.equals(fullDocument.value)) {
                    return fullDocument;
                }
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid ChangeStreamFullDocument", changeStreamFullDocument));
    }
}
