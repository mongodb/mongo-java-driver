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

package org.mongodb;

import org.bson.BSONWriter;
import org.bson.types.CodeWithScope;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mongodb.codecs.Codecs;

public class CodeWithScopeCodecTest {
    //CHECKSTYLE:OFF
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private BSONWriter bsonWriter;

    private CodeWithScopeCodec codeWithScopeCodec;

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        bsonWriter = context.mock(BSONWriter.class);
        codeWithScopeCodec = new CodeWithScopeCodec(Codecs.createDefault());
    }

    @Test
    public void shouldEncodeCodeWithScopeAsJavaScriptFollowedByDocumentOfScope() {
        final String javascriptCode = "<javascript code>";
        final CodeWithScope codeWithScope = new CodeWithScope(javascriptCode, new Document("the", "scope"));
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeJavaScriptWithScope(javascriptCode);
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("the");
            oneOf(bsonWriter).writeString("scope");
            oneOf(bsonWriter).writeEndDocument();
        }});

        codeWithScopeCodec.encode(bsonWriter, codeWithScope);
    }

}
