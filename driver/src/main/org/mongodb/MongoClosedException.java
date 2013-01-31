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

package org.mongodb;

/**
 * Exception indicating that an operation could not complete due to the {@code MongoClient} instance being
 * closed by another thread.
 */
public class MongoClosedException extends MongoClientException {
    private static final long serialVersionUID = -7887144401216210192L;

    /**
     * Constructs a new instance with the given message.
     * @param msg the message
     */
    public MongoClosedException(final String msg) {
        super(msg);
    }
}
