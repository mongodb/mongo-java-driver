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

import org.bson.BSONWriter;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.codecs.validators.FieldNameValidator;
import org.mongodb.codecs.validators.QueryFieldNameValidator;

import java.util.HashMap;
import java.util.Map;

public class MapCodecTest {

    //CHECKSTYLE:OFF
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private BSONWriter bsonWriter;

    private MapCodec mapCodec;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        bsonWriter = context.mock(BSONWriter.class);
        mapCodec = new MapCodec(Codecs.createDefault(), new FieldNameValidator());
    }

    @Test
    public void shouldEncodeStringToDocumentMap() {
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("myFieldName", new Document("doc", 1));

        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("myFieldName");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("doc");
            oneOf(bsonWriter).writeInt32(1);
            exactly(2).of(bsonWriter).writeEndDocument();
        }});

        mapCodec.encode(bsonWriter, map);
    }

    @Test
    public void shouldEncodeSimpleStringToObjectMap() {
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("myFieldName", "The Field");

        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("myFieldName");
            oneOf(bsonWriter).writeString("The Field");
            oneOf(bsonWriter).writeEndDocument();
        }});

        mapCodec.encode(bsonWriter, map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowDotsInKeysWhenValidatorIsCollectibleDocumentValidator() {
        mapCodec = new MapCodec(Codecs.createDefault(), new FieldNameValidator());

        final Map<String, Integer> mapWithInvalidFieldName = new HashMap<String, Integer>();
        mapWithInvalidFieldName.put("a.b", 1);

        //not sure this is correct, should you throw an exceptions halfway through writing?
        context.checking(new Expectations() {{
            allowing(bsonWriter);
        }});

        mapCodec.encode(bsonWriter, mapWithInvalidFieldName);
    }

    @Test
    public void shouldAllowDotsInKeysInNestedMapsWhenValidatorIsQueryDocumentValidator() {
        mapCodec = new MapCodec(Codecs.createDefault(), new QueryFieldNameValidator());
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("a.b", "The Field");

        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("a.b");
            oneOf(bsonWriter).writeString("The Field");
            oneOf(bsonWriter).writeEndDocument();
        }});

        mapCodec.encode(bsonWriter, map);
    }


    //TODO: Trish: optimise encoding primitive types?
}
