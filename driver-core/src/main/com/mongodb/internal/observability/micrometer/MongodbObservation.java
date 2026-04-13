/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.observability.micrometer;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.docs.ObservationDocumentation;

/**
 * MongoDB {@link ObservationDocumentation} definitions for operation-level and command-level observations.
 * <p>
 * These are split into two separate observation types so that each has a distinct name and a fixed set
 * of low-cardinality tag keys. This is required by Prometheus which rejects meters that share a name
 * but have different tag key sets.
 * </p>
 *
 * @since 5.7
 */
public enum MongodbObservation implements ObservationDocumentation {

    /**
     * Observation for high-level MongoDB operations (e.g. find, insert, update).
     * Created per user-initiated operation, may contain multiple command spans.
     */
    MONGODB_OPERATION {
        @Override
        public String getName() {
            return "mongodb.operation";
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return OperationLowCardinalityKeyNames.values();
        }
    },

    /**
     * Observation for wire-protocol MongoDB commands sent to the server.
     * Created per actual command (nested under an operation span).
     */
    MONGODB_COMMAND {
        @Override
        public String getName() {
            return "mongodb.command";
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return CommandLowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return HighCardinalityKeyNames.values();
        }
    };

    /**
     * Low cardinality key names for operation-level observations.
     */
    public enum OperationLowCardinalityKeyNames implements KeyName {

        SYSTEM {
            @Override
            public String asString() {
                return "db.system";
            }
        },
        NAMESPACE {
            @Override
            public String asString() {
                return "db.namespace";
            }
        },
        COLLECTION {
            @Override
            public String asString() {
                return "db.collection.name";
            }
        },
        OPERATION_NAME {
            @Override
            public String asString() {
                return "db.operation.name";
            }
        },
        OPERATION_SUMMARY {
            @Override
            public String asString() {
                return "db.operation.summary";
            }
        }
    }

    /**
     * Low cardinality key names for command-level observations.
     */
    public enum CommandLowCardinalityKeyNames implements KeyName {

        SYSTEM {
            @Override
            public String asString() {
                return "db.system";
            }
        },
        NAMESPACE {
            @Override
            public String asString() {
                return "db.namespace";
            }
        },
        COLLECTION {
            @Override
            public String asString() {
                return "db.collection.name";
            }
        },
        COMMAND_NAME {
            @Override
            public String asString() {
                return "db.command.name";
            }
        },
        QUERY_SUMMARY {
            @Override
            public String asString() {
                return "db.query.summary";
            }
        },
        NETWORK_TRANSPORT {
            @Override
            public String asString() {
                return "network.transport";
            }
        },
        SERVER_ADDRESS {
            @Override
            public String asString() {
                return "server.address";
            }
        },
        SERVER_PORT {
            @Override
            public String asString() {
                return "server.port";
            }
        },
        RESPONSE_STATUS_CODE {
            @Override
            public String asString() {
                return "db.response.status_code";
            }
        }
    }

    /**
     * High cardinality (highly variable values) key names for command-level observations.
     */
    public enum HighCardinalityKeyNames implements KeyName {

        QUERY_TEXT {
            @Override
            public String asString() {
                return "db.query.text";
            }
        },
        CLIENT_CONNECTION_ID {
            @Override
            public String asString() {
                return "db.mongodb.driver_connection_id";
            }
        },
        SERVER_CONNECTION_ID {
            @Override
            public String asString() {
                return "db.mongodb.server_connection_id";
            }
        },
        CURSOR_ID {
            @Override
            public String asString() {
                return "db.mongodb.cursor_id";
            }
        },
        TRANSACTION_NUMBER {
            @Override
            public String asString() {
                return "db.mongodb.txn_number";
            }
        },
        SESSION_ID {
            @Override
            public String asString() {
                return "db.mongodb.lsid";
            }
        },
        EXCEPTION_MESSAGE {
            @Override
            public String asString() {
                return "exception.message";
            }
        },
        EXCEPTION_TYPE {
            @Override
            public String asString() {
                return "exception.type";
            }
        },
        EXCEPTION_STACKTRACE {
            @Override
            public String asString() {
                return "exception.stacktrace";
            }
        }
    }
}
