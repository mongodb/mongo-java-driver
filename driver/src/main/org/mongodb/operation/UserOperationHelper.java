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

package org.mongodb.operation;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.Arrays;

import static com.mongodb.connection.NativeAuthenticationHelper.createAuthenticationHash;

final class UserOperationHelper {

    static BsonDocument asCommandDocument(final User user, final String commandName) {
        BsonDocument document = new BsonDocument();
        document.put(commandName, new BsonString(user.getCredential().getUserName()));
        document.put("pwd", new BsonString(createAuthenticationHash(user.getCredential().getUserName(),
                                                                    user.getCredential().getPassword())));
        document.put("digestPassword", BsonBoolean.FALSE);
        document.put("roles", new BsonArray(Arrays.<BsonValue>asList(new BsonString(getRoleName(user)))));
        return document;
    }

    private static String getRoleName(final User user) {
        return user.getCredential().getSource().equals("admin")
               ? (user.isReadOnly() ? "readAnyDatabase" : "root") : (user.isReadOnly() ? "read" : "dbOwner");
    }

    static BsonDocument asCollectionQueryDocument(final User user) {
        return new BsonDocument("user", new BsonString(user.getCredential().getUserName()));
    }

    static BsonDocument asCollectionDocument(final User user) {
        BsonDocument document = asCollectionQueryDocument(user);
        document.put("pwd", new BsonString(createAuthenticationHash(user.getCredential().getUserName(),
                                                                    user.getCredential().getPassword())));
        document.put("readOnly", BsonBoolean.valueOf(user.isReadOnly()));

        return document;
    }

    private UserOperationHelper() {
    }
}
