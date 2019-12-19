/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb

import org.bson.BSONEncoder
import org.bson.io.BasicOutputBuffer
import org.bson.io.OutputBuffer
import org.bson.types.BSONTimestamp
import org.bson.types.Binary
import org.bson.types.Code
import org.bson.types.MaxKey
import org.bson.types.MinKey
import org.bson.types.ObjectId
import org.bson.types.Symbol
import spock.lang.Specification

import static java.util.regex.Pattern.CASE_INSENSITIVE
import static java.util.regex.Pattern.compile

class LazyDBObjectSpecification extends Specification {
    BSONEncoder encoder = new DefaultDBEncoder()
    OutputBuffer buf = new BasicOutputBuffer()
    ByteArrayOutputStream bios;
    LazyDBDecoder lazyDBDecoder;
    DefaultDBDecoder defaultDBDecoder;

    def setup() {
        encoder.set(buf);
        bios = new ByteArrayOutputStream();
        lazyDBDecoder = new LazyDBDecoder();
        defaultDBDecoder = new DefaultDBDecoder();
    }

    def 'should lazily decode a DBRef'() {
        given:
        byte[] bytes = [
                44, 0, 0, 0, 3, 102, 0, 36, 0, 0, 0, 2, 36, 114, 101, 102,
                0, 4, 0, 0, 0, 97, 46, 98, 0, 7, 36, 105, 100, 0, 18, 52,
                86, 120, -112, 18, 52, 86, 120, -112, 18, 52, 0, 0,
        ]

        when:
        LazyDBObject document = new LazyDBObject(bytes, new LazyDBCallback(null))

        then:
        document['f'] instanceof DBRef
        document['f'] == new DBRef('a.b', new ObjectId('123456789012345678901234'))
    }

    def 'should lazily decode a DBRef with $db'() {
        given:
        byte[] bytes = [
                58, 0, 0, 0, 3, 102, 0, 50, 0, 0, 0, 2, 36, 114, 101, 102,
                0, 4, 0, 0, 0, 97, 46, 98, 0, 7, 36, 105, 100, 0, 18, 52,
                86, 120, -112, 18, 52, 86, 120, -112, 18, 52,
                2, 36, 100, 98, 0, 5, 0, 0, 0, 109, 121, 100, 98, 0, 0, 0
        ]

        when:
        LazyDBObject document = new LazyDBObject(bytes, new LazyDBCallback(null))

        then:
        document['f'] instanceof DBRef
        document['f'] == new DBRef('mydb', 'a.b', new ObjectId('123456789012345678901234'))
    }

    def testToString() throws IOException {
        given:
        DBObject origDoc = new BasicDBObject('x', true);
        encoder.putObject(origDoc);
        buf.pipe(bios);

        when:
        DBObject doc = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        then:
        doc.toString() == '{"x": true}'
    }

    def testDecodeAllTypes() throws IOException {
        given:
        DBObject origDoc = getTestDocument();
        encoder.putObject(origDoc);
        buf.pipe(bios);

        when:
        DBObject doc = defaultDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        then:
        assertDocsSame(origDoc, doc);
    }

    def testLazyDecodeAllTypes() throws InterruptedException, IOException {
        given:
        DBObject origDoc = getTestDocument();
        encoder.putObject(origDoc);
        buf.pipe(bios);

        when:
        DBObject doc = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        then:
        assertDocsSame(origDoc, doc);
    }

    def testMissingKey() throws IOException {
        given:
        encoder.putObject(getSimpleTestDocument());
        buf.pipe(bios);

        when:
        DBObject decodedObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        then:
        decodedObj['missingKey'] == null
    }

    def testKeySet() throws IOException {
        given:
        DBObject obj = getSimpleTestDocument();
        encoder.putObject(obj);
        buf.pipe(bios);

        when:
        DBObject decodedObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        then:
        decodedObj != null
        decodedObj instanceof LazyDBObject
        Set<String> keySet = decodedObj.keySet();

        keySet.size() == 6
        !keySet.isEmpty()

        keySet.toArray().length == 6

        def typedArray = keySet.toArray(new String[0]);
        typedArray.length == 6

        def array = keySet.toArray(new String[7]);
        array.length == 7
        array[6] == null

        keySet.contains('first')
        !keySet.contains('x')

        keySet.containsAll(['first', 'second', '_id', 'third', 'fourth', 'fifth'])
        !keySet.containsAll(['first', 'notFound'])

        obj['_id'] == decodedObj['_id']
        obj['first'] == decodedObj['first']
        obj['second'] == decodedObj['second']
        obj['third'] == decodedObj['third']
        obj['fourth'] == decodedObj['fourth']
        obj['fifth'] == decodedObj['fifth']
    }

    def testEntrySet() throws IOException {
        given:
        DBObject obj = getSimpleTestDocument();
        encoder.putObject(obj);
        buf.pipe(bios);

        when:
        DBObject decodedObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        then:
        Set<Map.Entry<String, Object>> entrySet = decodedObj.entrySet();
        entrySet.size() == 6
        !entrySet.isEmpty()

        entrySet.toArray().length == 6   // kind of a lame test

        Map.Entry<String, Object>[] typedArray = entrySet.toArray(new Map.Entry[entrySet.size()]);
        typedArray.length == 6

        def array = entrySet.toArray(new Map.Entry[7]);
        array.length == 7
        array[6] == null
    }

    def testPipe() throws IOException {
        given:
        DBObject obj = getSimpleTestDocument();
        encoder.putObject(obj);
        buf.pipe(bios);

        when:
        LazyDBObject lazyDBObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);
        bios.reset();
        int byteCount = lazyDBObj.pipe(bios);

        then:
        lazyDBObj.getBSONSize() == byteCount

        when:
        LazyDBObject lazyDBObjectFromPipe = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        then:
        lazyDBObj == lazyDBObjectFromPipe
    }

    def testLazyDBEncoder() throws IOException {
        // this is all set up just to get a lazy db object that can be encoded
        given:
        DBObject obj = getSimpleTestDocument();
        encoder.putObject(obj);
        buf.pipe(bios);
        LazyDBObject lazyDBObj = (LazyDBObject) lazyDBDecoder.decode(
                new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        // now to the actual test
        when:
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        int size = new LazyDBEncoder().writeObject(outputBuffer, lazyDBObj);

        then:
        lazyDBObj.getBSONSize() == size
        lazyDBObj.getBSONSize() == outputBuffer.size()

        // this is just asserting that the encoder actually piped the correct bytes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        lazyDBObj.pipe(baos);
        baos.toByteArray() == outputBuffer.toByteArray()
    }

    def getSimpleTestDocument() {
        [_id   : new ObjectId(),
         first : 1,
         second: 'str1',
         third : true,
         fourth : null,
         fifth: [firstNested: 1] as BasicDBObject] as BasicDBObject;
    }

    def getTestDocument() {
        [_id           : new ObjectId(),
         null          : null,
         max           : new MaxKey(),
         min           : new MinKey(),
         booleanTrue   : true,
         booleanFalse  : false,
         int1          : 1,
         int1500       : 1500,
         int3753       : 3753,
         tsp           : new BSONTimestamp(),
         date          : new Date(),
         long5         : 5L,
         long3254525   : 3254525L,
         float324_582  : 324.582f,
         double245_6289: 245.6289 as double,
         oid           : new ObjectId(),
         // Symbol wonky
         symbol        : new Symbol('foobar'),
         // Code wonky
         code          : new Code('var x = 12345;'),
         // TODO - Shell doesn't work with Code W/ Scope, return to this test later
         /*
         b.append( "code_scoped", new CodeWScope( "return x * 500;", test_doc ) );*/
         str           : 'foobarbaz',
         ref           : new DBRef('testRef', new ObjectId()),
         object        : ['abc', '12345'] as BasicDBObject,
         array         : ['foo', 'bar', 'baz', 'x', 'y', 'z'],
         binary        : new Binary('scott'.getBytes()),
         regex         : compile('^test.*regex.*xyz$', CASE_INSENSITIVE)] as BasicDBObject
    }

    void assertDocsSame(final DBObject origDoc, final DBObject doc) {
        assert doc['str'] == origDoc['str']
        assert doc['_id'] == origDoc['_id']
        assert doc['null'] == null
        assert doc['max'] == origDoc['max']
        assert doc['min'] == origDoc['min']
        assert doc['booleanTrue']
        assert !doc['booleanFalse']
        assert doc['int1'] == origDoc['int1']
        assert doc['int1500'] == origDoc['int1500']
        assert doc['int3753'] == origDoc['int3753']
        assert doc['tsp'] == origDoc['tsp']
        assert doc['date'] == doc['date']
        assert doc['long5'] == 5L
        assert doc['long3254525'] == 3254525L
        assert doc['float324_582'] == 324.5820007324219f
        assert doc['double245_6289'] == 245.6289 as double
        assert doc['oid'] == origDoc['oid']
        assert doc['str'] == 'foobarbaz'
        assert doc['ref'] == origDoc['ref']
        assert doc['object']['abc'] == origDoc['object']['abc']
        assert doc['array'][0] == 'foo'
        assert doc['array'][1] == 'bar'
        assert doc['array'][2] == 'baz'
        assert doc['array'][3] == 'x'
        assert doc['array'][4] == 'y'
        assert doc['array'][5] == 'z'
        assert doc['binary'] == origDoc['binary'].getData()
        assert doc['regex'].pattern() == origDoc['regex'].pattern()
        assert doc['regex'].flags() == origDoc['regex'].flags()
    }
}
