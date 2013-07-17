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
 */

package org.mongodb.connection;

public final class ResponseSettings {
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 0x2000000;   // 32MB

    private final int responseTo;
    private final int maxMessageSize;

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private int responseTo;
        private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;

        // CHECKSTYLE:OFF
        public Builder responseTo(final int responseTo) {
            this.responseTo = responseTo;
            return this;
        }

        public Builder maxMessageSize(final int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }

        public ResponseSettings build() {
            return new ResponseSettings(this);
        }
        // CHECKSTYLE:ON
    }

    public int getResponseTo() {
        return responseTo;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    ResponseSettings(final Builder builder) {
        this.responseTo = builder.responseTo;
        this.maxMessageSize = builder.maxMessageSize;
    }
}
