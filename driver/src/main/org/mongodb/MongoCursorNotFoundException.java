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
 * Exception thrown when a getmore is executed but the cursorId is no longer available on the server
 */
public class MongoCursorNotFoundException extends MongoException {
    private static final long serialVersionUID = 7890793341600725191L;
    private final ServerCursor cursor;

    public MongoCursorNotFoundException(final ServerCursor cursor) {
        super("The cursor was not found: " + cursor);
        this.cursor = cursor;
    }

    public ServerCursor getCursor() {
        return cursor;
    }
}
