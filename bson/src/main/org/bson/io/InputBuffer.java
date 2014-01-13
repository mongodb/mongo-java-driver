/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.bson.io;

import org.bson.BSONType;
import org.bson.types.ObjectId;

import java.io.Closeable;

// TODO: fix asymmetry where this is an interface and OutputBuffer is a class.
// Can this be an abstract class that extends InputStream?
public interface InputBuffer extends Closeable {
    int getPosition();

    boolean readBoolean();

    byte readByte();

    byte[] readBytes(int size);

    long readInt64();

    double readDouble();

    int readInt32();

    String readString();

    ObjectId readObjectId();

    BSONType readBSONType();

    String readCString();

    void skipCString();

    void skip(int numBytes);

    void close();
}
