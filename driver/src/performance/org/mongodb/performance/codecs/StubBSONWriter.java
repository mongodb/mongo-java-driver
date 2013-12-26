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

package org.mongodb.performance.codecs;

import org.bson.BSONWriter;
import org.bson.BSONWriterSettings;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;

class StubBSONWriter extends BSONWriter {
    private int numberOfIntsWritten;
    private int numberOfArraysStarted;
    private int numberOfArraysEnded;
    private int numberOfNamesWritten;
    private int numberOfStringsEncoded;

    StubBSONWriter() {
        super(new BSONWriterSettings());
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeBinaryData(final Binary binary) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeBoolean(final boolean value) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeDateTime(final long value) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeDouble(final double value) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeInt32(final int value) {
        numberOfIntsWritten++;
    }

    @Override
    public void writeInt64(final long value) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeJavaScript(final String code) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeJavaScriptWithScope(final String code) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeMaxKey() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeMinKey() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeNull() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeObjectId(final ObjectId objectId) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeRegularExpression(final RegularExpression regularExpression) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeString(final String value) {
        numberOfStringsEncoded++;
    }

    @Override
    public void writeSymbol(final String value) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeTimestamp(final BSONTimestamp value) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeUndefined() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeEndArray() {
        numberOfArraysEnded++;
    }

    @Override
    public void writeInt32(final String name, final int value) {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public void writeStartArray() {
        numberOfArraysStarted++;
    }

    @Override
    public void writeName(final String name) {
        numberOfNamesWritten++;
    }

    public int getNumberOfIntsWritten() {
        return numberOfIntsWritten;
    }

    int getNumberOfNamesWritten() {
        return numberOfNamesWritten;
    }

    int getNumberOfStringsEncoded() {
        return numberOfStringsEncoded;
    }
}
