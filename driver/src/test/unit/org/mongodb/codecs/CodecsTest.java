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

public class CodecsTest {
    //CHECKSTYLE:OFF
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private BSONWriter bsonWriter;
    private Codecs codecs;
    private PrimitiveCodecs primitiveCodecs;

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        bsonWriter = context.mock(BSONWriter.class);
        primitiveCodecs = context.mock(PrimitiveCodecs.class);

        codecs = Codecs.builder().primitiveCodecs(primitiveCodecs)
                                 .build();
    }

    @Test
    public void shouldDelegateEncodingOfPrimitiveTypes() {
        final String stringToEncode = "A String";
        context.checking(new Expectations() {{
            //TODO: kinda pointless?
            oneOf(primitiveCodecs).canEncode(String.class);
            will(returnValue(true));

            oneOf(primitiveCodecs).encode(bsonWriter, stringToEncode);
        }});

        codecs.encode(bsonWriter, stringToEncode);
    }

}
