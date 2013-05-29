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

package com.mongodb;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GroupTest extends DatabaseTestCase {

    @Before
    public void setUp() {
        super.setUp();
        collection.save(new BasicDBObject("x", "a"));
        collection.save(new BasicDBObject("x", "a"));
        collection.save(new BasicDBObject("x", "a"));
        collection.save(new BasicDBObject("x", "b"));
    }

    @Test
    public void testGroup() {
        final DBObject result = collection.group(
                new BasicDBObject("x", 1), null,
                new BasicDBObject("count", 0),
                "function( o , p ){ p.count++; }"
        );
        assertThat(result, instanceOf(BasicDBList.class));
        final BasicDBList list = (BasicDBList) result;
        assertEquals(2, list.size());
        assertThat(list, hasItem(new BasicDBObject("x", "a").append("count", 3.0)));
    }

    @Test
    public void testGroupWithGroupCommand() {
        final GroupCommand command = new GroupCommand(
                collection,
                new BasicDBObject("x", 1),
                null,
                new BasicDBObject("count", 0),
                "function( o , p ){ p.count++; }",
                null
        );

        final BasicDBList result = (BasicDBList) collection.group(command);
        assertEquals(2, result.size());
        assertThat(result, hasItem(new BasicDBObject("x", "a").append("count", 3.0)));
    }

    @Test
    public void testGroupWithCondition() {
        final BasicDBList result = (BasicDBList) collection.group(
                new BasicDBObject("x", 1),
                new BasicDBObject("x", "b"),
                new BasicDBObject("z", 0),
                "function(o,p){p.z = 2;}"
        );

        assertEquals(1, result.size());
        assertThat(result, hasItem(new BasicDBObject("x", "b").append("z", 2.0)));
    }

    @Test
    public void testGroupWithoutKey() {
        final BasicDBList result = (BasicDBList) collection.group(
                null,
                new BasicDBObject("x", "b"),
                new BasicDBObject("z", 0),
                "function(o,p){p.z = 2;}"
        );

        assertEquals(1, result.size());
        assertThat(result, hasItem(new BasicDBObject("z", 2.0)));
    }


    @Test
    public void testGroupWithFinalize() {
        final BasicDBList result = (BasicDBList) collection.group(
                new BasicDBObject("x", 1),
                new BasicDBObject("x", "b"),
                new BasicDBObject("z", 0),
                "function(o,p){p.z = 2;}",
                "function(o){return {a:1}}"
        );

        assertEquals(1, result.size());
        assertThat(result, hasItem(new BasicDBObject("a", 1.0)));
    }


    @Test(expected = CommandFailureException.class)
    public void testGroupWithoutInitialValue() {
        collection.group(
                new BasicDBObject("x", 1),
                new BasicDBObject("x", "b"),
                null,
                "function(o,p){p.z = 2;}"
        );
    }


    @Test(expected = CommandFailureException.class)
    public void testGroupWithoutReduce() {
        collection.group(
                new BasicDBObject("x", 1),
                new BasicDBObject("x", "b"),
                new BasicDBObject("z", 0),
                null
        );
    }

    @Test
    public void testGroupCommandWithPlainDBObject() {
        final DBObject command = new BasicDBObject()
                .append("key", new BasicDBObject("x", 1))
                .append("cond", null)
                .append("$reduce", "function( o , p ){ p.count++; }")
                .append("initial", new BasicDBObject("count", 0));

        final BasicDBList result = (BasicDBList) collection.group(command);
        assertEquals(2, result.size());
        assertThat(result, hasItem(new BasicDBObject("x", "a").append("count", 3.0)));
    }

    @Test
    public void testGroupCommandToDBObject() {
        final DBObject command = new GroupCommand(
                collection,
                new BasicDBObject("x", 1),
                null,
                new BasicDBObject("c", 0),
                "function(o,p){}",
                null
        ).toDBObject();

        final DBObject args = new BasicDBObject("ns", collection.getName())
                .append("key", new BasicDBObject("x", 1))
                .append("cond", null)
                .append("$reduce", "function(o,p){}")
                .append("initial", new BasicDBObject("c", 0));
        assertEquals(new BasicDBObject("group", args), command);
    }
}
