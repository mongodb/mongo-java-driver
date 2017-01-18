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

package org.bson.json;

class JsonBuffer {

    private final String buffer;
    private int position;
    private boolean eof;

    JsonBuffer(final String buffer) {
        this.buffer = buffer;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(final int position) {
        this.position = position;
    }

    public int read() {
        if (eof) {
            throw new JsonParseException("Trying to read past EOF.");
    } else if (position >= buffer.length()) {
            eof = true;
            return -1;
        }  else {
            return buffer.charAt(position++);
        }
    }

    public void unread(final int c) {
        eof = false;
        if (c != -1 && buffer.charAt(position - 1) == c) {
            position--;
        }
    }

    public String substring(final int beginIndex) {
        return buffer.substring(beginIndex);
    }

    public String substring(final int beginIndex, final int endIndex) {
        return buffer.substring(beginIndex, endIndex);
    }
}
