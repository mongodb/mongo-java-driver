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

package com.mongodb.embedded.client;

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;

import java.io.Closeable;
import java.io.File;

import static java.lang.String.format;
import static java.util.Arrays.asList;

final class EmbeddedDatabase implements Closeable {
    private static final Logger LOGGER = Loggers.getLogger("embedded.client");
    private static final String NATIVE_LIBRARY_NAME = "mongo_embedded_capi";
    private final MongoDBCAPI mongoDBCAPI;
    private volatile Pointer databasePointer;

    EmbeddedDatabase(final MongoClientSettings mongoClientSettings) {
        File directory = new File(mongoClientSettings.getDbPath());
        try {
            if (directory.mkdirs() && LOGGER.isInfoEnabled()) {
                LOGGER.info(format("Created dbpath directory: %s", mongoClientSettings.getDbPath()));
            }
        } catch (SecurityException e) {
            throw new MongoException(format("Could not validate / create the dbpath: %s", mongoClientSettings.getDbPath()));
        }

        if (mongoClientSettings.getLibraryPath() != null) {
            NativeLibrary.addSearchPath(NATIVE_LIBRARY_NAME, mongoClientSettings.getLibraryPath());
        }
        try {
            this.mongoDBCAPI = Native.loadLibrary(NATIVE_LIBRARY_NAME, MongoDBCAPI.class);
        } catch (UnsatisfiedLinkError e) {
            throw new MongoClientException(format("Failed to load the mongodb library: '%s'."
                    + "%n %s %n"
                    + "%n Please set the library location by either:"
                    + "%n - Adding it to the classpath."
                    + "%n - Setting 'jna.library.path' system property"
                    + "%n - Configuring it in the 'MongoClientSettings.builder().libraryPath' method."
                    + "%n", NATIVE_LIBRARY_NAME, e.getMessage()), e);
        }

        String[] cmdArgs = createCmdArgs(mongoClientSettings);
        try {
            this.databasePointer = mongoDBCAPI.libmongodbcapi_db_new(cmdArgs.length, cmdArgs, createEnvArgs());
            if (databasePointer == null) {
                throw new MongoException("Could not create a new embedded database");
            }
        } catch (Throwable t) {
            throw new MongoException("Error from embedded server when calling db_new: " + t.getMessage(), t);
        }
    }

    public EmbeddedConnection createConnection() {
        return new EmbeddedConnection(mongoDBCAPI, databasePointer);
    }

    public void pump() {
        try {
            int errorCode = mongoDBCAPI.libmongodbcapi_db_pump(databasePointer);
            if (errorCode != 0) {
                throw new MongoException(errorCode, "Error from embedded server: + " + errorCode);
            }
        } catch (Throwable t) {
            throw new MongoException("Error from embedded server when calling db_pump: " + t.getMessage(), t);
        }
    }

    @Override
    public void close() {
        if (databasePointer != null) {
            try {
                mongoDBCAPI.libmongodbcapi_db_destroy(databasePointer);
            } catch (Throwable t) {
                throw new MongoException("Error from embedded server when calling db_destroy: " + t.getMessage(), t);
            }
            databasePointer = null;
        }
    }

    private String[] createCmdArgs(final MongoClientSettings mongoClientSettings) {
        return asList(
                "--port=0",
                "--storageEngine=mobile",
                "--dbpath=" + mongoClientSettings.getDbPath()
        ).toArray(new String[0]);
    }

    private String[] createEnvArgs() {
        return new String[0];
    }
}
