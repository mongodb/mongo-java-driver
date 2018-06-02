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

import com.sun.jna.Library;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;

interface MongoDBCAPI extends Library {


    // CHECKSTYLE.OFF: MethodName
    /**
     * Creates a status pointer
     *
     * @return the status pointer from which to get an associated status code / error information.
     */
    Pointer mongo_embedded_v1_status_create();

    /**
     * Destroys a valid status pointer object.
     *
     * @param status the `mongo_embedded_v1_status` pointer to release.
     */

    void mongo_embedded_v1_status_destroy(Pointer status);

    /**
     * Gets an error code from a status pointer.
     *
     * @param status the `mongo_embedded_v1_status` pointer from which to get an associated status code.
     * @return The error code associated with the `status` parameter or 0 if successful.
     *
     */
    int mongo_embedded_v1_status_get_error(Pointer status);

    /**
     * Gets a descriptive error message from a status pointer.
     *
     * @param status the `mongo_embedded_v1_status` pointer from which to get an associated error message.
     *
     * @return A null-terminated string containing an error message.
     */
    String mongo_embedded_v1_status_get_explanation(Pointer status);

    /**
     * Gets a status code from a status pointer.
     *
     * @param status the `mongo_embedded_v1_status` pointer from which to get an associated status code.
     * @return A numeric status code associated with the `status` parameter which indicates a
     * sub-category of failure.
     */
    int mongo_embedded_v1_status_get_code(Pointer status);

    /**
     * Initializes the mongodbcapi library, required before any other call.
     *
     * <p>Cannot be called again without {@link #mongo_embedded_v1_lib_fini(Pointer, Pointer)} being called first.</p>
     *
     * @param initParams the embedded mongod initialization parameters.
     * @param status the `mongo_embedded_v1_status` pointer from which to get an associated status code.
     * @return the lib pointer or null if there was a failure. Modifies the status on failure.
     */
    Pointer mongo_embedded_v1_lib_init(Structure initParams, Pointer status);

    /**
     * Tears down the state of the library, all databases must be closed before calling this.
     *
     * @param lib the `mongo_embedded_v1_lib` pointer
     * @param status the `mongo_embedded_v1_status` pointer from which to get an associated status code.
     * @return the error code or 0 if successful. Modifies the status on failure.
     */
    int mongo_embedded_v1_lib_fini(Pointer lib, Pointer status);

    /**
     * Creates an embedded MongoDB instance and returns a handle with the service context.
     *
     * @param lib the `mongo_embedded_v1_lib` pointer
     * @param yamlConfig null-terminated YAML formatted MongoDB configuration string
     * @param status the `mongo_embedded_v1_status` pointer from which to get an associated status code.
     * @return the instance pointer or null if there was a failure. Modifies the status on failure.
     */
    Pointer mongo_embedded_v1_instance_create(Pointer lib, String yamlConfig, Pointer status);

    /**
     * Shuts down an embedded MongoDB instance.
     *
     * @param instance the `mongo_embedded_v1_instance` pointer to be destroyed.
     * @param status the `mongo_embedded_v1_status` pointer from which to get an associated status code.
     * @return the error code or 0 if successful. Modifies the status on failure.
     */
    int mongo_embedded_v1_instance_destroy(Pointer instance, Pointer status);

    /**
     * Create a new embedded client instance.
     *
     * @param instance the `mongo_embedded_v1_instance` pointer.
     * @param status the `mongo_embedded_v1_status` pointer from which to get an associated status code.
     * @return the client pointer or null if there was a failure. Modifies the status on failure.
     */
    Pointer mongo_embedded_v1_client_create(Pointer instance, Pointer status);

    /**
     * Destroys an embedded client instance.
     *
     * @param client the `mongo_embedded_v1_client` pointer.
     * @param status the `mongo_embedded_v1_status` pointer from which to get an associated status code.
     * @return the error code or 0 if successful. Modifies the status on failure.
     */
    int mongo_embedded_v1_client_destroy(Pointer client, Pointer status);

    /**
     * Make an RPC call.  Not clear yet on whether this is the correct signature.
     *
     * @param client     the `mongo_embedded_v1_client` pointer
     * @param input      the RPC input
     * @param inputSize  the RPC input size
     * @param output     the RPC output
     * @param outputSize the RPC output size
     * @param status     status the `mongo_embedded_v1_status` pointer from which to get an associated status code.
     * @return the error code or 0 if successful. Modifies the status on failure.
     */
    int mongo_embedded_v1_client_invoke(Pointer client, byte[] input, int inputSize, PointerByReference output, IntByReference outputSize,
                                     Pointer status);


}
