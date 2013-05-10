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

import org.bson.BSONBinaryReader;
import org.bson.BSONWriter;
import org.bson.types.CodeWithScope;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mongodb.DBRef;
import org.mongodb.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.codecs.CodecTestUtil.prepareReaderWithObjectToBeDecoded;

public class CodecsTest {
    //CHECKSTYLE:OFF
    @Rule
    public final JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private BSONWriter bsonWriter;

    private Codecs codecs;

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        context.setThreadingPolicy(new Synchroniser());
        bsonWriter = context.mock(BSONWriter.class);

        codecs = Codecs.builder().primitiveCodecs(PrimitiveCodecs.createDefault())
                       .build();
    }

    @Test
    public void shouldEncodeCodeWithScopeAsJavaScriptFollowedByDocumentOfScopeWhenPassedInAsObject() {
        final String javascriptCode = "<javascript code>";
        final Object codeWithScope = new CodeWithScope(javascriptCode, new Document("the", "scope"));
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeJavaScriptWithScope(javascriptCode);
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("the");
            oneOf(bsonWriter).writeString("scope");
            oneOf(bsonWriter).writeEndDocument();
        }});

        codecs.encode(bsonWriter, codeWithScope);
    }

    @Test
    public void shouldDecodeCodeWithScope() {
        final String javascriptCode = "{javascript code}";
        final Document theScope = new Document("the", "scope");

        final CodeWithScope codeWithScope = new CodeWithScope(javascriptCode, theScope);
        final BSONBinaryReader reader = prepareReaderWithObjectToBeDecoded(codeWithScope);

        final Object actualCodeWithScope = codecs.decode(reader);

        final CodeWithScope expectedCodeWithScope = new CodeWithScope(javascriptCode, theScope);
        assertThat((CodeWithScope) actualCodeWithScope, is(expectedCodeWithScope));
    }

    @Test
    public void shouldEncodeDbRefWhenDisguisedAsAnObject() {
        final String namespace = "theNamespace";
        final String theId = "TheId";
        final Object dbRef = new DBRef(theId, namespace);
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeString("$ref", namespace);
            oneOf(bsonWriter).writeName("$id");
            oneOf(bsonWriter).writeString(theId);
            oneOf(bsonWriter).writeEndDocument();
        }});

        codecs.encode(bsonWriter, dbRef);
    }

    @Test
    public void shouldEncodeNull() {
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeNull();
        }});
        codecs.encode(bsonWriter, (Object) null);
    }

    @Test
    public void shouldBeAbleToEncodeMap() {
        assertThat(codecs.canEncode(new HashMap<String, Object>()), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeArray() {
        assertThat(codecs.canEncode(new String[]{}), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeList() {
        assertThat(codecs.canEncode(new ArrayList<String>()), is(true));
    }

    @Test
    public void shouldBeAbleToEncodePrimitive() {
        assertThat(codecs.canEncode(1), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeCodeWithScope() {
        assertThat(codecs.canEncode(new CodeWithScope(null, null)), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeDBRef() {
        assertThat(codecs.canEncode(new DBRef(null, null)), is(true));
    }

    @Test
    public void shouldBeAbleToEncodeNull() {
        assertThat(codecs.canEncode(null), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeMap() {
        assertThat(codecs.canDecode(Map.class), is(true));
    }

    @Test
    public void shouldBeAbleToDecodeHashMap() {
        assertThat(codecs.canDecode(HashMap.class), is(true));
    }

    @Test
    @Ignore("not implemented yet")
    public void shouldBeAbleToDecodePrimitive() {
        assertThat(codecs.canDecode(int.class), is(true));
    }

}
