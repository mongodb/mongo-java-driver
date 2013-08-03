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

package org.mongodb;

import org.mongodb.connection.ServerAddress;
import org.mongodb.operation.MongoServerException;

public class MongoQueryFailureException extends MongoServerException {
    private static final long serialVersionUID = -5113350133297015801L;
    private final Document errorDocument;

    public MongoQueryFailureException(final ServerAddress address, final Document errorDocument) {
        super("Query failed with error code " + getErrorCode(errorDocument) + " and error message + '"
              + getErrorMessage(errorDocument) + "' on server " + address, address);
        this.errorDocument = errorDocument;
    }

    // TODO: Create bean for the error document similar to CommandResult
    public Document getErrorDocument() {
        return errorDocument;
    }

    public int getErrorCode() {
        return getErrorCode(errorDocument);
    }

    @Override
    public String getErrorMessage() {
        return getErrorMessage(errorDocument);
    }

    private static String getErrorMessage(final Document errorDocument) {
        return (String) errorDocument.get("$err");
    }

    private static int getErrorCode(final Document errorDocument) {
        return (Integer) errorDocument.get("code");
    }
}
