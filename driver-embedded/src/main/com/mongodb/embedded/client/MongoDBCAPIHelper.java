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
import com.sun.jna.Callback;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

final class MongoDBCAPIHelper {
    private static final Logger LOGGER = Loggers.getLogger("embedded.server");
    private static final String NATIVE_LIBRARY_NAME = "mongo_embedded_capi";
    private static volatile MongoDBCAPI mongoDBCAPI;

    static synchronized void checkHasBeenInitialized() {
        checkInitialized();
    }

    // CHECKSTYLE.OFF: MethodName
    static synchronized void init(final MongoEmbeddedSettings mongoEmbeddedSettings) {
        if (mongoDBCAPI != null) {
            throw new MongoClientException("MongoDBCAPI has been initialized but not closed");
        }

        if (mongoEmbeddedSettings.getLibraryPath() != null) {
            NativeLibrary.addSearchPath(NATIVE_LIBRARY_NAME, mongoEmbeddedSettings.getLibraryPath());
        }

        try {
            mongoDBCAPI = Native.loadLibrary(NATIVE_LIBRARY_NAME, MongoDBCAPI.class);
        } catch (UnsatisfiedLinkError e) {
            throw new MongoClientException(format("Failed to load the mongodb library: '%s'."
                    + "%n %s %n"
                    + "%n Please set the library location by either:"
                    + "%n - Adding it to the classpath."
                    + "%n - Setting 'jna.library.path' system property"
                    + "%n - Configuring it in the 'MongoEmbeddedSettings.builder().libraryPath' method."
                    + "%n", NATIVE_LIBRARY_NAME, e.getMessage()), e);
        }
        try {
            validateErrorCode(mongoDBCAPI.libmongodbcapi_init(new MongoDBCAPIInitParams(mongoEmbeddedSettings)));
        } catch (Throwable t) {
            throw createError("init", t);
        }
    }

    static synchronized void fini() {
        checkInitialized();
        try {
            validateErrorCode(mongoDBCAPI.libmongodbcapi_fini());
            mongoDBCAPI = null;
        } catch (Throwable t) {
            throw createError("fini", t);
        }
    }

    static Pointer db_new(final String yamlConfig) {
        checkInitialized();
        try {
            return mongoDBCAPI.libmongodbcapi_db_new(yamlConfig);
        } catch (Throwable t) {
            throw createError("db_new", t);
        }
    }

    static void db_destroy(final Pointer db) {
        checkInitialized();
        try {
            mongoDBCAPI.libmongodbcapi_db_destroy(db);
        } catch (Throwable t) {
            throw createError("db_destroy", t);
        }
    }

    static void db_pump(final Pointer db) {
        checkInitialized();
        try {
            validateErrorCode(mongoDBCAPI.libmongodbcapi_db_pump(db));
        } catch (Throwable t) {
            throw createError("db_pump", t);
        }
    }

    static Pointer db_client_new(final Pointer db) {
        checkInitialized();
        try {
            return mongoDBCAPI.libmongodbcapi_db_client_new(db);
        } catch (Throwable t) {
            throw createError("db_client_new", t);
        }
    }

    static void db_client_destroy(final Pointer client) {
        checkInitialized();
        try {
            mongoDBCAPI.libmongodbcapi_db_client_destroy(client);
        } catch (Throwable t) {
            throw createError("db_client_destroy", t);
        }
    }

    static void db_client_wire_protocol_rpc(final Pointer client, final byte[] input, final int inputSize,
                                                         final PointerByReference output, final IntByReference outputSize) {
        checkInitialized();
        try {
            validateErrorCode(mongoDBCAPI.libmongodbcapi_db_client_wire_protocol_rpc(client, input, inputSize, output, outputSize));
        } catch (Throwable t) {
            throw createError("db_client_wire_protocol_rpc", t);
        }
    }

    static int get_last_error() {
        checkInitialized();
        try {
            return mongoDBCAPI.libmongodbcapi_get_last_error();
        } catch (Throwable t) {
            throw createError("get_last_error", t);
        }
    }

    private static MongoException createError(final String methodName, final Throwable t) {
        return new MongoException(format("Error from embedded server when calling '%s': %s", methodName, t.getMessage()), t);
    }

    private static void validateErrorCode(final int errorCode) {
        if (errorCode != 0) {
            String errorMessage;
            if (errorCode == 1) {
                errorMessage = "The embedded server has already been initialized";
            } else if (errorCode == 2) {
                errorMessage = "The embedded server has not been initialized";
            } else if (errorCode == 3) {
                errorMessage = "The embedded database has not been closed";
            } else {
                errorMessage = "An unknown error occured";
            }
            throw new MongoException(format("%s. Error Code(%s).", errorMessage, errorCode));
        }
    }

    private static void checkInitialized() {
        if (mongoDBCAPI == null) {
            throw new MongoClientException("MongoDBCAPI has not been initialized");
        }
    }

    private MongoDBCAPIHelper() {
    }

    /**
     * Represents libmongodbcapi_init_params
     */
    public static class MongoDBCAPIInitParams extends Structure {
        // CHECKSTYLE.OFF: VisibilityModifier
        public String yamlConfig;
        public long logFlags;
        public Callback logCallback;
        public String userData;
        // CHECKSTYLE.ON: VisibilityModifier

        MongoDBCAPIInitParams(final MongoEmbeddedSettings settings) {
            super();
            this.yamlConfig = settings.getYamlConfig();
            this.logFlags = settings.getLogLevel().getLevel();
            this.logCallback = settings.getLogLevel() == MongoEmbeddedSettings.LogLevel.LOGGER ? new LogCallback() : null;
        }

        protected List<String> getFieldOrder() {
            return asList("yamlConfig", "logFlags", "logCallback", "userData");
        }
    }

    static class LogCallback implements Callback {

        public void apply(final String userData, final String message, final String component, final String context,
                          final int severity) {
            String logMessage = format("%-9s [%s] %s", component.toUpperCase(), context, message).trim();

            if (severity < -2) {
                LOGGER.error(logMessage);   // Severe/Fatal & Error messages
            } else if (severity == -2) {
                LOGGER.warn(logMessage);    // Warning messages
            } else if (severity < 1) {
                LOGGER.info(logMessage);    // Info / Log messages
            } else {
                LOGGER.debug(logMessage);   // Debug messages
            }
        }
    }
}
