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
     * Initializes the mongodbcapi library, required before any other call. Cannot be called again
     * without libmongodbcapi_fini() being called first.
     *
     * @param initParams the embedded mongod initialization parameters.

     * @note This function is not thread safe.
     * @return the error, or 0 if success
     */
    int libmongodbcapi_init(Structure initParams);

    /**
     * Tears down the state of the library, all databases must be closed before calling this.
     *
     * @return the error, or 0 if success
     */
    int libmongodbcapi_fini();

    /**
     * Create a new db instance.
     *
     * @param yamlConfig null-terminated YAML formatted MongoDB configuration string
     * @return the db pointer
     */
    Pointer libmongodbcapi_db_new(String yamlConfig);

    /**
     * Destroy a db instance.
     *
     * @param db the db pointer
     */
    void libmongodbcapi_db_destroy(Pointer db);

    /**
     * Pump the message queue.
     *
     * @param db the db pointer
     * @return the error, or 0 if success
     */
    int libmongodbcapi_db_pump(Pointer db);

    /**
     * Create a new client instance.
     *
     * @param db the db pointer
     * @return the client pointer
     */
    Pointer libmongodbcapi_db_client_new(Pointer db);

    /**
     * Destroy a client instance.
     *
     * @param client the client pointer
     */
    void libmongodbcapi_db_client_destroy(Pointer client);

    /**
     * Make an RPC call.  Not clear yet on whether this is the correct signature.
     *
     * @param client     the client pointer
     * @param input      the RPC input
     * @param inputSize  the RPC input size
     * @param output     the RPC output
     * @param outputSize the RPC output size
     * @return the error, or 0 if success
     */
    int libmongodbcapi_db_client_wire_protocol_rpc(Pointer client, byte[] input, int inputSize, PointerByReference output,
                                                   IntByReference outputSize);

    /**
     * Gets the last error.
     *
     * @return the last error
     */
    int libmongodbcapi_get_last_error();
}
