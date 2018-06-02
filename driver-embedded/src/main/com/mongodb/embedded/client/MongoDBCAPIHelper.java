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
    private static final LogCallback LOG_CALLBACK = new LogCallback();
    private static volatile MongoDBCAPI mongoDBCAPI;
    private static volatile Pointer libraryStatusPointer;
    private static volatile Pointer libraryPointer;


    static synchronized void checkHasBeenInitialized() {
        checkInitialized();
    }

    // CHECKSTYLE.OFF: MethodName
    static synchronized void init(final MongoEmbeddedSettings mongoEmbeddedSettings) {
        if (mongoDBCAPI != null) {
            throw new MongoClientEmbeddedException("MongoDBCAPI has been initialized but not closed");
        }

        if (mongoEmbeddedSettings.getLibraryPath() != null) {
            NativeLibrary.addSearchPath(NATIVE_LIBRARY_NAME, mongoEmbeddedSettings.getLibraryPath());
        }

        try {
            mongoDBCAPI = Native.loadLibrary(NATIVE_LIBRARY_NAME, MongoDBCAPI.class);
        } catch (UnsatisfiedLinkError e) {
            throw new MongoClientEmbeddedException(format("Failed to load the mongodb library: '%s'."
                    + "%n %s %n"
                    + "%n Please set the library location by either:"
                    + "%n - Adding it to the classpath."
                    + "%n - Setting 'jna.library.path' system property"
                    + "%n - Configuring it in the 'MongoEmbeddedSettings.builder().libraryPath' method."
                    + "%n", NATIVE_LIBRARY_NAME, e.getMessage()), e);
        }
        libraryStatusPointer = createStatusPointer();
        libraryPointer = mongoDBCAPI.mongo_embedded_v1_lib_init(new MongoDBCAPIInitParams(mongoEmbeddedSettings), libraryStatusPointer);
        if (libraryPointer == null) {
            createErrorFromStatus(libraryStatusPointer);
        }
    }

    static synchronized void fini() {
        checkInitialized();
        try {
            validateErrorCode(libraryStatusPointer, mongoDBCAPI.mongo_embedded_v1_lib_fini(libraryPointer, libraryStatusPointer));
            libraryPointer = null;
        } catch (Throwable t) {
            throw createError("fini", t);
        }
        destroyStatusPointer(libraryStatusPointer);
        libraryStatusPointer = null;
        mongoDBCAPI = null;
    }

    static Pointer instance_create(final String yamlConfig, final Pointer instanceStatusPointer) {
        checkInitialized();
        try {
            return validatePointerCreated(instanceStatusPointer, mongoDBCAPI.mongo_embedded_v1_instance_create(libraryPointer, yamlConfig,
                    instanceStatusPointer)
            );
        } catch (Throwable t) {
            throw createError("instance_create", t);
        }
    }

    static void instance_destroy(final Pointer instance, final Pointer instanceStatusPointer) {
        checkInitialized();
        try {
            validateErrorCode(instanceStatusPointer, mongoDBCAPI.mongo_embedded_v1_instance_destroy(instance, instanceStatusPointer));
        } catch (Throwable t) {
            throw createError("instance_destroy", t);
        }
    }

    static Pointer create_client(final Pointer instance, final Pointer clientStatusPointer) {
        checkInitialized();
        try {
            return validatePointerCreated(clientStatusPointer, mongoDBCAPI.mongo_embedded_v1_client_create(instance, clientStatusPointer));
        } catch (Throwable t) {
            throw createError("client_create", t);
        }
    }

    static void client_destroy(final Pointer client, final Pointer clientStatusPointer) {
        checkInitialized();
        try {
            validateErrorCode(clientStatusPointer, mongoDBCAPI.mongo_embedded_v1_client_destroy(client, clientStatusPointer));
        } catch (Throwable t) {
            throw createError("client_destroy", t);
        }
    }

    static void client_invoke(final Pointer client, final byte[] input, final PointerByReference output, final IntByReference outputSize,
                              final Pointer clientStatusPointer) {
        checkInitialized();
        try {
            validateErrorCode(clientStatusPointer, mongoDBCAPI.mongo_embedded_v1_client_invoke(client, input, input.length, output,
                    outputSize, clientStatusPointer)
            );
        } catch (Throwable t) {
            throw createError("client_invoke", t);
        }
    }

    static Pointer createStatusPointer() {
        try {
            return mongoDBCAPI.mongo_embedded_v1_status_create();
        } catch (Throwable t) {
            throw createError("status_create", t);
        }
    }

    static void destroyStatusPointer(final Pointer statusPointer) {
        try {
            mongoDBCAPI.mongo_embedded_v1_status_destroy(statusPointer);
        } catch (Throwable t) {
            throw createError("status_destroy", t);
        }
    }

    private static MongoClientEmbeddedException createError(final String methodName, final Throwable t) {
        if (t instanceof MongoClientEmbeddedException) {
            return (MongoClientEmbeddedException) t;
        }
        return new MongoClientEmbeddedException(format("Error from embedded server when calling '%s': %s", methodName, t.getMessage()), t);
    }

    private static void createErrorFromStatus(final Pointer statusPointer) {
        createErrorFromStatus(statusPointer, mongoDBCAPI.mongo_embedded_v1_status_get_error(statusPointer));
    }

    private static void createErrorFromStatus(final Pointer statusPointer, final int errorCode) {
        throw new MongoClientEmbeddedException(errorCode,
                mongoDBCAPI.mongo_embedded_v1_status_get_code(statusPointer),
                mongoDBCAPI.mongo_embedded_v1_status_get_explanation(statusPointer));
    }

    private static Pointer validatePointerCreated(final Pointer statusPointer, final Pointer pointer) {
        if (pointer == null) {
           createErrorFromStatus(statusPointer);
        }
        return pointer;
    }

    private static void validateErrorCode(final Pointer statusPointer, final int errorCode) {
        if (errorCode != 0) {
            createErrorFromStatus(statusPointer, errorCode);
        }
    }

    private static void checkInitialized() {
        if (mongoDBCAPI == null || libraryPointer == null || libraryStatusPointer == null) {
            throw new MongoClientEmbeddedException("MongoDBCAPI has not been initialized");
        }
    }

    private MongoDBCAPIHelper() {
    }

    /**
     * Represents mongo_embedded_v1_init_params
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
            this.logCallback = settings.getLogLevel() == MongoEmbeddedSettings.LogLevel.LOGGER ? LOG_CALLBACK : null;
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
