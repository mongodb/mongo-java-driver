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

package org.bson.json;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class JsonStringBuffer implements JsonBuffer {

    private final String buffer;
    private int position;
    private List<Integer> markedPoses;
    private boolean eof;
    private int lastChar;

    JsonStringBuffer(final String buffer) {
        this.buffer = buffer;
        markedPoses = new ArrayList<Integer>();
    }

    public int getPosition() {
        return position;
    }

    public int read() {
        if (eof) {
            throw new JsonParseException("Trying to read past EOF.");
    } else if (position >= buffer.length()) {
            eof = true;
            return -1;
        }  else {
            final int currChar = buffer.charAt(position++);
            lastChar = currChar;
            return currChar;
        }
    }

    public void unread(final int c) {
        eof = false;
        if (c != -1 && lastChar != -1 && lastChar == c) {
            lastChar = -1;
            position--;
        }
    }

    public int mark() {
        markedPoses.add(position);
        return position;
    }

    public void reset(final int markPos) {
        if (markedPoses.isEmpty()) {
            return;
        }
        final int idx = markedPoses.indexOf(markPos);
        if (idx == -1) {
            throw new IllegalArgumentException("mark invalidated");
        }
        if (markPos > position) {
            throw new IllegalStateException("mark cannot reset ahead of position, only back");
        }
        final Iterator<Integer> markIter = markedPoses.iterator();
        while (markIter.hasNext()) {
            final Integer nextMark = markIter.next();
            if (nextMark > markPos) {
                markIter.remove();
            }
        }
        markedPoses.remove(idx);
        position = markPos;
    }

    public void discard(final int markPos) {
        final int idx = markedPoses.indexOf(markPos);
        if (idx == -1) {
            return;
        }
        markedPoses.remove(idx);
    }

    public void close() {

    }
}
