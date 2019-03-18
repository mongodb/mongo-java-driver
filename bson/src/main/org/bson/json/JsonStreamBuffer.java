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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class JsonStreamBuffer implements JsonBuffer {

    private final BufferedReader stream;
    private int position;
    private int lastChar;
    private int bufferStartPos;
    private boolean reuseLastChar;
    private boolean eof;
    private List<Integer> buffer;
    private List<Integer> markedPoses;

    JsonStreamBuffer(final InputStream stream) {
        try {
            this.stream = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
        } catch (final UnsupportedEncodingException e) {
            throw new JsonParseException(e);
        }
        buffer = new ArrayList<Integer>();
        markedPoses = new LinkedList<Integer>();
        bufferStartPos = -1;
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
            final int reusedChar = lastChar;
            lastChar = -1;
            position++;
            return reusedChar;
        }

        // use the buffer until we catch up to the stream position
        if (position >= bufferStartPos && position - bufferStartPos < buffer.size()) {
            final int currChar = buffer.get(position - bufferStartPos);
            lastChar = currChar;
            position++;
            return currChar;
        }

        // if there are no marked positions and we have caught up, we can clear the buffer
        if (markedPoses.isEmpty()) {
            bufferStartPos = -1;
            buffer.clear();
        }

        // otherwise, try and read from the stream
        final int currChar;
        try {
            currChar = stream.read();
            lastChar = currChar;

            // if the lowest mark is ahead of our position, we can safely
            if (!markedPoses.isEmpty()) {
                buffer.add(currChar);
            }
        } catch (final IOException e) {
            throw new JsonParseException(e);
        }
        position++;
        if (currChar == -1) {
            eof = true;
        }
        return currChar;
    }

    public void unread(final int c) {
        eof = false;
        if (c != -1 && lastChar == c) {
            reuseLastChar = true;
            position--;
        }
    }

    public int mark() {
        if (buffer.isEmpty()) {
            bufferStartPos = position;
        }
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
        if (reuseLastChar && markPos != position) {
            reuseLastChar = false;
        }
        markedPoses.remove(idx);
        position = markPos;
    }

    public void discard(final int markPos) {
        if (markedPoses.isEmpty()) {
            return;
        }
        final int idx = markedPoses.indexOf(markPos);
        if (idx == -1) {
            return;
        }
        markedPoses.remove(idx);
    }

    public void close() {
        try {
            stream.close();
        } catch (IOException e) {
            throw new JsonParseException(e);
        }
        buffer.clear();
    }
}
