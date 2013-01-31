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
 *
 */

package com.mongodb;

/**
 * An enumeration of the supported authentication mechanisms.
 */
public enum MongoAuthenticationMechanism {
    /**
     * The GSSAPI protocol, to support Kerberos v5 via a SASL-based authentication protocol
     */
    GSSAPI {
        /**
         * The default source for GSSAPI is a reserved name that doesn't correspond to any database.
         * @return the default source.
         */
        @Override
        public String getDefaultSource() {
            return "$external";
        }

        @Override
        public String getMechanismName() {
            return "GSSAPI";
        }
    },
    /**
     * The native MongoDB authentication mechanism.  This is an abbreviation for MongoDB Challenge Response.
     */
    MONGO_CR {
        /**
         * The default source is the "admin" database.
         * @return the "admin" database
         */
        @Override
        public String getDefaultSource() {
            return "admin";
        }

        @Override
        public String getMechanismName() {
            return "MONGO-CR";
        }
    };

    /**
     * Gets the default source for this mechanism, usually a database name.
     *
     * @return the default database for this protocol
     */
    public abstract String getDefaultSource();

    /**
     * Gets the mechanism name.
     * @return the name
     */
    public abstract String getMechanismName();

    public static MongoAuthenticationMechanism byMechanismName(String name) {
        if (name.equals(GSSAPI.getMechanismName())) {
            return GSSAPI;
        }
        if (name.equals(MONGO_CR.getMechanismName())) {
            return MONGO_CR;
        }
        throw new IllegalArgumentException("Invalid authentication mechanism name: " + name);
    }
}
