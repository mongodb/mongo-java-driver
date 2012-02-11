/**
 * Copyright (C) 2012 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson;

// Bson
import org.bson.types.ObjectId;

// TestNg
import org.testng.Assert;
import org.testng.annotations.Test;


public class NewBSONDecoderTest extends Assert {


    public void testDecoder(final String pName, final BSONDecoder pDecoder) throws Exception {

        final BasicBSONObject origDoc = new BasicBSONObject("_id", ObjectId.get());
        origDoc.put("long", Long.MAX_VALUE);
        origDoc.put("int", Integer.MAX_VALUE);
        origDoc.put("string", "yay... we are finally making this faster");


        final BasicBSONObject origNested = new BasicBSONObject("id", ObjectId.get());
        origNested.put("long", Long.MAX_VALUE);
        origNested.put("int", Integer.MAX_VALUE);
        origDoc.put("nested", origNested);

        final byte [] orig = BSON.encode(origDoc);

        int count = 500000;
        //int count = 1000000;
        //int count = 100;

        long startTime = System.currentTimeMillis();

        for (int idx=0; idx < count; idx++) {
            final BasicBSONObject doc = (BasicBSONObject)pDecoder.readObject(orig);
            assertEquals(origDoc.getLong("long"), doc.getLong("long"));
            assertEquals(origDoc.getInt("int"), doc.getInt("int"));

            assertEquals(origDoc.getString("string"), doc.getString("string"));

            //System.out.println("--- ok: " + doc.getString("string"));

            assertEquals(origDoc.getObjectId("_id"), doc.getObjectId("_id"));

            final BasicBSONObject nested = (BasicBSONObject)doc.get("nested");

            assertEquals(origNested.getLong("long"), nested.getLong("long"));
            assertEquals(origNested.getInt("int"), nested.getInt("int"));
            assertEquals(origNested.getObjectId("_id"), nested.getObjectId("_id"));
        }

        //System.out.println(pName + ": " + (System.currentTimeMillis() - startTime));
    }

    @Test
    public void testNewDecoder1() throws Exception {
        final NewBSONDecoder decoder = new NewBSONDecoder();
        testDecoder("new", decoder);
    }

    @Test
    public void testNewDecoderCreate1() throws Exception {
        long startTime = System.currentTimeMillis();
        for (int idx=0; idx < 1000000; idx++) {
            final NewBSONDecoder decoder = new NewBSONDecoder();
        }
        //System.out.println("new create 1: " + (System.currentTimeMillis() - startTime));
    }

    @Test
    public void testCurrentDecoderCreate1() throws Exception {
        long startTime = System.currentTimeMillis();
        for (int idx=0; idx < 1000000; idx++) {
            final BasicBSONDecoder decoder = new BasicBSONDecoder();
        }
        //System.out.println("current create 1: " + (System.currentTimeMillis() - startTime));
    }

    @Test
    public void testCurrent1() throws Exception {
        final BasicBSONDecoder decoder = new BasicBSONDecoder();
        testDecoder("current", decoder);
    }

    @Test
    public void testNewDecoder2() throws Exception {
        final NewBSONDecoder decoder = new NewBSONDecoder();
        testDecoder("new", decoder);
    }

    @Test
    public void testCurrent2() throws Exception {
        final BasicBSONDecoder decoder = new BasicBSONDecoder();
        testDecoder("current", decoder);
    }
}

