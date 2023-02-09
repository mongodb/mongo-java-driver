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
package com.mongodb;

import com.mongodb.annotations.Beta;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * An exception thrown by methods that may automatically create data encryption keys
 * where needed based on the {@code encryptedFields} configuration.
 *
 * @since 4.9
 */
@Beta(Beta.Reason.SERVER)
public final class MongoUpdatedEncryptedFieldsException extends MongoClientException {
    private static final long serialVersionUID = 1;

    private final BsonDocument encryptedFields;

    /**
     * Not part of the public API.
     *
     * @param encryptedFields The (partially) updated {@code encryptedFields} document,
     * which allows users to infer which data keys are known to be created before the exception happened
     * (see {@link #getEncryptedFields()} for more details).
     * Reporting this back to a user may be helpful because creation of a data key includes persisting it in the key vault.
     * @param msg The message.
     * @param cause The cause.
     */
    public MongoUpdatedEncryptedFieldsException(final BsonDocument encryptedFields, final String msg, final Throwable cause) {
        super(msg, assertNotNull(cause));
        this.encryptedFields = assertNotNull(encryptedFields);
    }

    /**
     * The {@code encryptedFields} document that allows inferring which data keys are <strong>known to be created</strong>
     * before {@code this} exception happened by comparing this document with the original {@code encryptedFields} configuration.
     * Creation of a data key includes persisting it in the key vault.
     * <p>
     * Note that the returned {@code encryptedFields} document is not guaranteed to contain information about all the data keys that
     * may be created, only about those that the driver is certain about. For example, if persisting a data key times out,
     * the driver does not know whether it can be considered created or not, and does not include the information about the key in
     * the {@code encryptedFields} document. You can analyze whether the {@linkplain #getCause() cause} is a definite or indefinite
     * error, and rely on the returned {@code encryptedFields} to be containing information on all created keys
     * only if the error is definite.</p>
     *
     * @return The updated {@code encryptedFields} document.
     */
    public BsonDocument getEncryptedFields() {
        return encryptedFields;
    }
}
