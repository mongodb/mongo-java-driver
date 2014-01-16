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

package com.mongodb;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.Assert.fail;

public class OutMessageTest {

    Mongo m;

    @Before
    public void setup() throws UnknownHostException {
        m = new MongoClient();
    }

    // Ensure defensive code is in place after doneWithMessage is called.
    @Test
    public void testDoneWithMessage() throws IOException {
        DBCollection collection = m.getDB("OutMessageTest").getCollection("doneWithMessage");

        OutMessage om = OutMessage.insert(collection, DefaultDBEncoder.FACTORY.create(), WriteConcern.SAFE);
        om.putObject(new BasicDBObject("_id", new ObjectId()));

        // This will release the buffer and put the object in an unusable state.
        om.doneWithMessage();

        try {
            om.doneWithMessage();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }

        try {
            om.prepare();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }

        try {
            om.putObject(new BasicDBObject("_id", new ObjectId()));
            fail();
        } catch (IllegalStateException e) {
            // expected
        }

        try {
            om.pipe(new ByteArrayOutputStream(100));
            fail();
        } catch (IllegalStateException e) {
            // expected
        }

        try {
            om.size();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }

    }
}
