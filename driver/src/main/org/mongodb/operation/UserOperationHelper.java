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

package org.mongodb.operation;

import org.mongodb.Document;
import org.mongodb.connection.NativeAuthenticationHelper;

import java.util.Arrays;

final class UserOperationHelper {

    static Document asCommandDocument(final User user, final String commandName) {
        return new Document(commandName, user.getCredential().getUserName())
                   .append("pwd", new String(user.getCredential().getPassword()))
                   .append("roles", Arrays.asList(getRoleName(user)));
    }

    // TODO: use different roles when source is "admin"
    private static String getRoleName(final User user) {
        return user.isReadOnly() ? "read" : "readWrite";
    }

    static Document asCollectionQueryDocument(final User user) {
        return new Document("user", user.getCredential().getUserName());
    }

    static Document asCollectionDocument(final User user) {
        return asCollectionQueryDocument(user)
                   .append("pwd", NativeAuthenticationHelper.createAuthenticationHash(user.getCredential().getUserName(),
                                                                                      user.getCredential().getPassword()))
                   .append("readOnly", user.isReadOnly());

    }

    private UserOperationHelper() {
    }
}
