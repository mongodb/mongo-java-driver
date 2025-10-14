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

package com.mongodb.internal.tracing;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.docs.ObservationDocumentation;

/**
 * A MongoDB-based {@link Observation}.
 *
 * @since 5.7
 */
public enum MongodbObservation implements ObservationDocumentation {

    MONGODB_OBSERVATION {
        @Override
        public String getName() {
            return "mongodb";
        }

        @Override
        public KeyName[] getLowCardinalityKeyNames() {
            return LowCardinalityKeyNames.values();
        }

        @Override
        public KeyName[] getHighCardinalityKeyNames() {
            return HighCardinalityKeyNames.values();
        }

    };

    /**
     * Enums related to low cardinality key names for MongoDB tags.
     */
    public enum LowCardinalityKeyNames implements KeyName {

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
        COMMAND_NAME {
            @Override
            public String asString() {
                return "db.command.name";
            }
        },
        NETWORK_TRANSPORT {
            @Override
            public String asString() {
                return "network.transport";
            }
        },
        OPERATION_SUMMARY {
            @Override
            public String asString() {
                return "db.operation.summary";
            }
        },
        QUERY_SUMMARY {
            @Override
            public String asString() {
                return "db.query.summary";
            }
        },
        CURSOR_ID {
            @Override
            public String asString() {
                return "db.mongodb.cursor_id";
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
        SERVER_TYPE {
            @Override
            public String asString() {
                return "server.type";
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
        EXCEPTION_STACKTRACE {
            @Override
            public String asString() {
                return "exception.stacktrace";
            }
        },
        EXCEPTION_TYPE {
            @Override
            public String asString() {
                return "exception.type";
            }
        },
        EXCEPTION_MESSAGE {
            @Override
            public String asString() {
                return "exception.message";
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
     * Enums related to high cardinality (highly variable values) key names for MongoDB tags.
     */
    public enum HighCardinalityKeyNames implements KeyName {

        QUERY_TEXT {
            @Override
            public String asString() {
                return "db.query.text";
            }
        }
    }
}
