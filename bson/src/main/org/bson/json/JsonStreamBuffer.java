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

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

class JsonStreamBuffer implements JsonBuffer {

    private final Reader reader;
    private final List<Integer> markedPositions = new ArrayList<>();
    private final int initialBufferSize;
    private int position;
    private int lastChar;
    private boolean reuseLastChar;
    private boolean eof;
    private char[] buffer;
    private int bufferStartPos;
    private int bufferCount;

    JsonStreamBuffer(final Reader reader) {
        this(reader, 16);
    }

    JsonStreamBuffer(final Reader reader, final int initialBufferSize) {
        this.initialBufferSize = initialBufferSize;
        this.reader = reader;
        resetBuffer();
    }

    public int getPosition() {
        return position;
    }

    public int read() {
        if (eof) {
            throw new JsonParseException("Trying to read past EOF.");
        }

        // if we just unread, we need to use the last character read since it may not be in the
        // buffer
        if (reuseLastChar) {
            reuseLastChar = false;
            int reusedChar = lastChar;
            lastChar = -1;
            position++;
            return reusedChar;
        }

        // use the buffer until we catch up to the stream position
        if (position - bufferStartPos < bufferCount) {
            int currChar = buffer[position - bufferStartPos];
            lastChar = currChar;
            position++;
            return currChar;
        }

        if (markedPositions.isEmpty()) {
            resetBuffer();
        }

        // otherwise, try and read from the stream
        try {
            int nextChar = reader.read();
            if (nextChar != -1) {
                lastChar = nextChar;
                addToBuffer((char) nextChar);
            }
            position++;
            if (nextChar == -1) {
                eof = true;
            }
            return nextChar;

        } catch (final IOException e) {
            throw new JsonParseException(e);
        }
    }

    private void resetBuffer() {
        bufferStartPos = -1;
        bufferCount = 0;
        buffer = new char[initialBufferSize];
    }

    public void unread(final int c) {
        eof = false;
        if (c != -1 && lastChar == c) {
            reuseLastChar = true;
            position--;
        }
    }

    public int mark() {
        if (bufferCount == 0) {   // Why not markedPositions.isEmpty()?
            bufferStartPos = position;
        }
        if (!markedPositions.contains(position)) {
            markedPositions.add(position);
        }
        return position;
    }

    public void reset(final int markPos) {
        if (markPos > position) {
            throw new IllegalStateException("mark cannot reset ahead of position, only back");
        }
        int idx = markedPositions.indexOf(markPos);
        if (idx == -1) {
            throw new IllegalArgumentException("mark invalidated");
        }
        if (markPos != position) {
            reuseLastChar = false;
        }
        markedPositions.subList(idx, markedPositions.size()).clear();
        position = markPos;
    }

    public void discard(final int markPos) {
        int idx = markedPositions.indexOf(markPos);
        if (idx == -1) {
            return;
        }
        markedPositions.subList(idx, markedPositions.size()).clear();
    }

    private void addToBuffer(final char curChar) {
        // if the lowest mark is ahead of our position, we can safely add it to our buffer
        if (!markedPositions.isEmpty()) {
            if (bufferCount == buffer.length) {
                char[] newBuffer = new char[buffer.length * 2];
                System.arraycopy(buffer, 0, newBuffer, 0, bufferCount);
                buffer = newBuffer;
            }
            buffer[bufferCount] = curChar;
            bufferCount++;
        }
    }
}
