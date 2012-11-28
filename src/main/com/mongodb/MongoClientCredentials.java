/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

/**
 * Represents credentials to authenticate to a mongo server.
 * <p>
 * Note: This constructor is provisional and is subject to change before the final release
 */
public class MongoClientCredentials {
    public static final String MONGODB_MECHANISM = "mongodb";
    public static final String GSSAPI_MECHANISM = "GSSAPI";

    private final String mechanism;
    private final String userName;
    private final char[] password;
    private final String database;


    public MongoClientCredentials(final String userName, final char[] password) {
        this.userName = userName;
        this.password = password;
        database = null;
        mechanism = MONGODB_MECHANISM;
    }

    public MongoClientCredentials(final String userName, final char[] password, String database) {
        this.userName = userName;
        this.password = password;
        this.database = database;
        mechanism = MONGODB_MECHANISM;
    }

    public MongoClientCredentials(final String userName, final String mechanism) {
        this.userName = userName;
        this.mechanism = mechanism;
        this.password = null;
        this.database = null;
    }

    public String getMechanism() {
        return mechanism;
    }

    public String getUserName() {
        return userName;
    }

    public char[] getPassword() {
        return password.clone();
    }

    public String getDatabase() {
        return database;
    }
}
