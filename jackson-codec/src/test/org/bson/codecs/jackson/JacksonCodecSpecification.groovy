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

package org.bson.codecs.jackson

import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDocumentWriter
import org.bson.BsonDouble
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.bson.BsonSymbol
import org.bson.BsonTimestamp
import org.bson.BsonValue
import org.bson.codecs.DecoderContext
import org.bson.codecs.EncoderContext
import org.bson.types.ObjectId
import spock.lang.Shared
import spock.lang.Specification

import java.util.regex.Pattern

/**
 * Created by guo on 7/16/14.
 */
class JacksonCodecSpecification extends Specification {

    @Shared oid
    @Shared a
    @Shared d
    @Shared bsonDoc

    def setupSpec() {
        oid = new ObjectId();
        a = new ArrayList<BsonValue>();
        a.add(new BsonInt32(10));
        a.add(new BsonNull());
        a.add(new BsonArray(new ArrayList<BsonValue>()))
        a.add(new BsonString("this is a string in an array"));
        d = new Date();

        bsonDoc = new BsonDocument([
                new BsonElement('js', new BsonJavaScript("var a = 1;")),
                new BsonElement('symbol', new BsonSymbol("someSymbol")),
                new BsonElement('ts', new BsonTimestamp(10,20)),
                new BsonElement('regex', new BsonRegularExpression("pattern","0")),
                new BsonElement('date', new BsonDateTime(d.getTime())),
                new BsonElement('oid', new BsonObjectId(oid)),
                new BsonElement('_id', new BsonString('this is an unique ID')),
                new BsonElement('string', new BsonString("this is a string")),
                new BsonElement('nulls', new BsonNull()),
                new BsonElement('integer', new BsonInt32(-1)),
                new BsonElement('longs', new BsonInt64(-2L)),
                new BsonElement('doubles', new BsonDouble(-3.0)),
                new BsonElement('booleans', new BsonBoolean(true)),
                new BsonElement('arrays', new BsonArray(a)),
                new BsonElement('obj', new BsonDocument([
                        new BsonElement('js', new BsonNull()),
                        new BsonElement('symbol', new BsonNull()),
                        new BsonElement('ts', new BsonNull()),
                        new BsonElement('regex', new BsonNull()),
                        new BsonElement('date', new BsonNull()),
                        new BsonElement('oid', new BsonNull()),
                        new BsonElement('_id', new BsonNull()),
                        new BsonElement('string', new BsonNull()),
                        new BsonElement('nulls', new BsonNull()),
                        new BsonElement('integer', new BsonNull()),
                        new BsonElement('longs', new BsonNull()),
                        new BsonElement('doubles', new BsonDouble(-4.0)),
                        new BsonElement('booleans', new BsonNull()),
                        new BsonElement('arrays', new BsonNull()),
                        new BsonElement('obj', new BsonNull()),
                ]))
        ])
    }



    def 'should encode all POJOs'() {
        given:
        // TODO: this seems like duplicating the tests in ParserSpecification and ParsreSpecification. Is this still necessary?

        def codec = new JacksonCodec<MockObject>(MockObject.class);



        def doc = new BsonDocument();
        def writer  = new BsonDocumentWriter(doc);
        def context = EncoderContext.builder().build();

        when:
        def obj = new MockObject(true);
        obj.oid = oid;
        obj.date = d;
        codec.encode(writer,obj,context);

        then:
        bsonDoc == doc;

    }

    def 'should decode all POJOs'() {
        given:

        def codec = new JacksonCodec<MockObject>(MockObject.class);

        def bsonReader = new BsonDocumentReader(bsonDoc);
        def context = DecoderContext.builder().build();


        when:
        def expectedDoc = new MockObject(true);
        expectedDoc.oid = oid;
        expectedDoc.date = d;
        def decodedDoc = codec.decode(bsonReader,context);

        then:
        expectedDoc == decodedDoc;
    }
}
