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

package org.mongodb.json;

public class JSONBuffer {

    private final String buffer;
    private int position;

    public JSONBuffer(String buffer) {
        this.buffer = buffer;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int read() {
        return (position >= buffer.length()) ? -1 : buffer.codePointAt(position++);
    }

    public void unread(int c) {
        if (c != -1 && buffer.codePointAt(position - 1) == c) {
            position--;
        }
    }

    public String substring(int beginIndex) {
        return buffer.substring(beginIndex);
    }

    public String substring(int beginIndex, int endIndex) {
        return buffer.substring(beginIndex, endIndex);
    }
}
