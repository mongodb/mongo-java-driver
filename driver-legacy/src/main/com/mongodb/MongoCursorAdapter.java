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

package com.mongodb;


import com.mongodb.client.MongoCursor;

class MongoCursorAdapter implements Cursor {
    private final MongoCursor<DBObject> cursor;

    MongoCursorAdapter(final MongoCursor<DBObject> cursor) {
        this.cursor = cursor;
    }

    @Override
    public int available() {
        return cursor.available();
    }

    @Override
    public long getCursorId() {
        ServerCursor serverCursor = cursor.getServerCursor();
        if (serverCursor == null) {
            return 0;
        }
        return serverCursor.getId();
    }

    @Override
    public ServerAddress getServerAddress() {
        return cursor.getServerAddress();
    }

    @Override
    public void close() {
        cursor.close();
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public DBObject next() {
        return cursor.next();
    }

    @Override
    public void remove() {
        cursor.remove();
    }
}
