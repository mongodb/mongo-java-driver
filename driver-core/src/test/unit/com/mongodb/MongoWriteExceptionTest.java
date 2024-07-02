/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright (c) 2008-2014 Atlassian Pty Ltd
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

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MongoWriteExceptionTest {

    @TempDir
    Path tempDir;

    @Test
    public void testExceptionProperties() {
        WriteError writeError = new WriteError(11000, "Duplicate key", new BsonDocument("x", new BsonInt32(1)));
        MongoWriteException e = new MongoWriteException(writeError, new ServerAddress("host1"), Collections.emptySet());

        assertEquals("Write operation error on server host1:27017. Write error: WriteError{code=11000, message='Duplicate key', "
                + "details={\"x\": 1}}.",
                e.getMessage());
        assertEquals(writeError.getCode(), e.getCode());
        assertEquals(writeError, e.getError());
    }

    @Test
    public void testExceptionSerializable() throws Exception {
        WriteError writeError = new WriteError(11000, "Duplicate key", new BsonDocument("x", new BsonInt32(1)));
        MongoWriteException writeException = new MongoWriteException(writeError, new ServerAddress("host1"), Collections.emptySet());


        Path tempPath = tempDir.resolve("testFile");
        try (FileOutputStream fileOutputStream = new FileOutputStream(tempPath.toFile());
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(writeException);
        }

        try (FileInputStream fileInputStream = new FileInputStream(tempPath.toFile());
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            MongoWriteException deserializedException = (MongoWriteException) objectInputStream.readObject();
            assertEquals(writeException.getError(), deserializedException.getError());
        }
    }
}
