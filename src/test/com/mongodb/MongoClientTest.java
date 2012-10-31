/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import junit.framework.Assert;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.Arrays;

public class MongoClientTest {
    @Test
    public void testConstructors() throws UnknownHostException {
        MongoClient mc;

        mc = new MongoClient();
        Assert.assertEquals(WriteConcern.ACKNOWLEDGED, mc.getWriteConcern());
        mc.close();

        mc = new MongoClient("localhost");
        Assert.assertEquals(WriteConcern.ACKNOWLEDGED, mc.getWriteConcern());
        mc.close();

        mc = new MongoClient("localhost", new MongoClientOptions.Builder().build());
        Assert.assertEquals(WriteConcern.ACKNOWLEDGED, mc.getWriteConcern());
        mc.close();

        mc = new MongoClient("localhost", 27017);
        Assert.assertEquals(WriteConcern.ACKNOWLEDGED, mc.getWriteConcern());
        mc.close();

        mc = new MongoClient(new ServerAddress("localhost"));
        Assert.assertEquals(WriteConcern.ACKNOWLEDGED, mc.getWriteConcern());
        mc.close();

        mc = new MongoClient(new ServerAddress("localhost"), new MongoClientOptions.Builder().build());
        Assert.assertEquals(WriteConcern.ACKNOWLEDGED, mc.getWriteConcern());
        mc.close();

        mc = new MongoClient(Arrays.asList(new ServerAddress("localhost")));
        Assert.assertEquals(WriteConcern.ACKNOWLEDGED, mc.getWriteConcern());
        mc.close();

        mc = new MongoClient(Arrays.asList(new ServerAddress("localhost")), new MongoClientOptions.Builder().build());
        Assert.assertEquals(WriteConcern.ACKNOWLEDGED, mc.getWriteConcern());
        mc.close();

        mc = new MongoClient(new MongoClientURI("mongodb://localhost"));
        Assert.assertEquals(WriteConcern.ACKNOWLEDGED, mc.getWriteConcern());
        mc.close();
    }
}
