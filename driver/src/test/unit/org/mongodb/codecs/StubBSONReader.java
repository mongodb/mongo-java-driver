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

package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.BSONReaderSettings;
import org.bson.BSONType;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;

public class StubBSONReader extends BSONReader {
    protected StubBSONReader() {
        super(new BSONReaderSettings());
        setState(State.NAME);
    }

    @Override
    public Binary readBinaryData() {
        return null;
    }

    @Override
    public boolean readBoolean() {
        return false;
    }

    @Override
    public BSONType readBSONType() {
        return null;
    }

    @Override
    public long readDateTime() {
        return 0L;
    }

    @Override
    public double readDouble() {
        return 0.0;
    }

    @Override
    public void readEndArray() {
    }

    @Override
    public void readEndDocument() {
    }

    @Override
    public int readInt32() {
        return 0;
    }

    @Override
    public long readInt64() {
        return 0L;
    }

    @Override
    public String readJavaScript() {
        return null;
    }

    @Override
    public String readJavaScriptWithScope() {
        return null;
    }

    @Override
    public void readMaxKey() {
    }

    @Override
    public void readMinKey() {
    }

    @Override
    public void readNull() {
    }

    @Override
    public ObjectId readObjectId() {
        return null;
    }

    @Override
    public RegularExpression readRegularExpression() {
        return null;
    }

    @Override
    public void readStartArray() {
    }

    @Override
    public void readStartDocument() {
    }

    @Override
    public String readString() {
        return null;
    }

    @Override
    public String readSymbol() {
        return null;
    }

    @Override
    public BSONTimestamp readTimestamp() {
        return null;
    }

    @Override
    public void readUndefined() {
    }

    @Override
    public void skipName() {
    }

    @Override
    public void skipValue() {
    }
}
