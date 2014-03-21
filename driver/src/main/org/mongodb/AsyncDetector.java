/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb;

import static org.mongodb.AsyncDetector.StreamType.NETTY;
import static org.mongodb.AsyncDetector.StreamType.NIO2;
import static org.mongodb.AsyncDetector.StreamType.NONE;

final class AsyncDetector {

    enum StreamType {
        NONE("none"),
        // NIO2-based asynchronous stream implementation
        NIO2("nio2"),
        // Netty 4.0-based  asynchronous stream implementation
        NETTY("netty");
        private final String name;

        StreamType(final String name) {
            this.name = name;
        }

        private String getName() {
            return name;
        }
    }

    private static final StreamType STREAM_TYPE;

    private AsyncDetector() {
    }

    static {
        if (System.getProperty("org.mongodb.disableAsync", "false").equals("true")) {
            STREAM_TYPE = NONE;
        } else {
            String streamTypeString = System.getProperty("org.mongodb.async.type", NIO2.getName());
            if (streamTypeString.equals(NETTY.getName())) {
                STREAM_TYPE = NETTY;
            } else if (streamTypeString.equals(NIO2.getName()) && isJava7OrGreater()) {
                STREAM_TYPE = NIO2;
            } else {
                STREAM_TYPE = NONE;
            }
        }
    }

    private static boolean isJava7OrGreater() {
        String javaSpecificationVersion = System.getProperty("java.specification.version");
        String minorVersionString = javaSpecificationVersion.substring(javaSpecificationVersion.lastIndexOf(".") + 1);
        try {
            return Integer.parseInt(minorVersionString) >= 7;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    static boolean isAsyncEnabled() {
        return STREAM_TYPE != NONE;
    }

    static StreamType getAsyncStreamType() {
        return STREAM_TYPE;
    }
}
