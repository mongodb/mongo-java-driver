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

package com.mongodb.internal.tracing;

/**
 * Contains constant tag names used for tracing and monitoring MongoDB operations.
 * These tags are typically used to annotate spans or events with relevant metadata.
 *
 * @since 5.6
 */
public final class Tags {
    private Tags() {
    }

    public static final String SYSTEM = "db.system";
    public static final String NAMESPACE = "db.namespace";
    public static final String COLLECTION = "db.collection.name";
    public static final String OPERATION_NAME = "db.operation.name";
    public static final String COMMAND_NAME = "db.command.name";
    public static final String NETWORK_TRANSPORT = "network.transport";
    public static final String OPERATION_SUMMARY = "db.operation.summary";
    public static final String QUERY_SUMMARY = "db.query.summary";
    public static final String QUERY_TEXT = "db.query.text";
    public static final String CURSOR_ID = "db.mongodb.cursor_id";
    public static final String SERVER_ADDRESS = "server.address";
    public static final String SERVER_PORT = "server.port";
    public static final String SERVER_TYPE = "server.type";
    public static final String CLIENT_CONNECTION_ID = "db.mongodb.driver_connection_id";
    public static final String SERVER_CONNECTION_ID = "db.mongodb.server_connection_id";
    public static final String TRANSACTION_NUMBER = "db.mongodb.txnNumber";
    public static final String SESSION_ID = "db.mongodb.lsid";
    public static final String EXCEPTION_STACKTRACE = "exception.stacktrace";
    public static final String EXCEPTION_TYPE = "exception.type";
    public static final String EXCEPTION_MESSAGE = "exception.message";
}
