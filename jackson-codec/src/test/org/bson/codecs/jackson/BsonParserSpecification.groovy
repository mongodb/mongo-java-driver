package org.bson.codecs.jackson

import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDocumentReader
import org.bson.BsonDouble
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.BsonType
import org.bson.types.ObjectId
import spock.lang.Specification

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue

/**
 * Created by guo on 7/18/14.
 */
class BsonParserSpecification extends Specification {


    def 'should parse primitives'() {

        given:
        def doc = new BsonDocument([
                new BsonElement('string', new BsonString('this is a string')),
                new BsonElement('nulls', new BsonNull()),
                new BsonElement('integer', new BsonInt32(-1)),
                new BsonElement('longs', new BsonInt64(-2L)),
                new BsonElement('doubles', new BsonDouble(-3.0)),
                new BsonElement('booleans', new BsonBoolean(true)),
        ]);
        def reader = new BsonDocumentReader(doc);
        def parser = new JacksonBsonParser(reader);

        def names = ["string","nulls","integer","longs","doubles","booleans"]
        def values = ["this is a string", null, -1, -2L, -3.0, true];

        when:
        parser.getCurrentToken(); //init the parser
        def token = parser.nextToken();

        then:
        int i = 0;
        while (i < names.size()) {
            if (token == JsonToken.FIELD_NAME) {
                token == names[i]
            } else {
                parser.getCurrentValue() == values[i]
                i++
            }
            token = parser.nextToken();
        }


    }

    def 'should return null when parse beyond end'() {
        given:
        def doc = new BsonDocument("myField", new BsonString("myValue"))
        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)

        when:
        parser.getCurrentToken();

        then:
        int i = 0
        while (parser.nextToken() != null) {
            ++i
            i <= 3
        }
        3 == i

    }

    def 'should return big string'() {
        given:
        def bigStr = new StringBuilder();
        for (int i = 0; i < 80000; i++) {
            bigStr.append("abc");
        }
        def doc = new BsonDocument("string", new BsonString(bigStr.toString()))
        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)

        when:
        parser.getCurrentToken();
        parser.nextToken();

        then:
        240000 == parser.getValueAsString().length();

    }

    def 'should return bigInt and bigDecimal'() {
        given:

        BsonDocument doc = new BsonDocument([
                new BsonElement("double", new BsonDouble(5.0)),
                new BsonElement("int32", new BsonInt32(1234))
        ])
        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        mapper.configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true);

        when:
        Map<?, ?> data = mapper.readValue(parser, Map.class);

        then:
        BigDecimal.class == data.get("double").getClass()
        BigInteger.class == data.get("int32").getClass()

    }

    def 'should return embedded documents'() {
        given:

        def doc = new BsonDocument([
                new BsonElement("int32",new BsonInt32(5)),
                new BsonElement("obj",new BsonDocument(
                        "int64", new BsonInt64(10L)
                )),
                new BsonElement("string", new BsonString("hello"))
        ])

        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)
        ObjectMapper mapper = new ObjectMapper();

        when:
        Map<?, ?> data = mapper.readValue(parser, Map.class);

        then:
        3 == data.size();
        5 == data.get("int32");
        Map<?, ?> data2 = (Map<?, ?>)data.get("obj");
        1 == data2.size();
        10L == data2.get("int64");
        "hello" == data.get("string");

    }

    def 'should return embeded arrays'() {
        given:

        def doc = new BsonDocument([
                new BsonElement("int32",new BsonInt32(5)),
                new BsonElement("arr",new BsonArray([
                    new BsonInt32(5),
                    new BsonInt32(6)
                ])),
                new BsonElement("string", new BsonString("hello"))
        ])

        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)
        ObjectMapper mapper = new ObjectMapper();

        when:
        Map<?, ?> data = mapper.readValue(parser, Map.class);

        then:
        3 == data.size();
        5 == data.get("int32");

    }

    def 'should return text'() {
        given:

        BsonDocument doc = new BsonDocument([
                new BsonElement("double", new BsonDouble(5.0)),
                new BsonElement("int32", new BsonInt32(1234))
        ])

        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)

        when:
        parser.getCurrentToken();

        then:
        JsonToken.FIELD_NAME == parser.nextToken();
        "double" == parser.getCurrentName();
        JsonToken.VALUE_NUMBER_FLOAT == parser.nextToken();
        5.0f == parser.getFloatValue();
        "5.0" == parser.getText();
        JsonToken.FIELD_NAME == parser.nextToken();
        "int32" == parser.getCurrentName();
        JsonToken.VALUE_NUMBER_INT == parser.nextToken();
        1234 == parser.getIntValue();
        "1234" == parser.getText();
        JsonToken.END_OBJECT == parser.nextToken();
    }

    def 'should return binary'() {
        given:
        byte[] b = [1, 2, 3, 4, 5 ]
        BsonDocument doc = new BsonDocument([
                new BsonElement("byte", new BsonBinary(b)),
        ])

        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)

        when:
        parser.getCurrentToken()
        parser.nextToken();

        then:
        b == parser.getBinaryValue()
    }

    def 'should return objectId'() {
        given:
        def objectId = new ObjectId()
        BsonDocument doc = new BsonDocument([
                new BsonElement("byte", new BsonObjectId(objectId)),
        ])

        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)

        when:
        parser.getCurrentToken()
        parser.nextToken();

        then:
        objectId == parser.getObjectId()
    }

    def 'should return timestamp'() {
        given:
        def ts = new BsonTimestamp(10,5)
        BsonDocument doc = new BsonDocument([
                new BsonElement("byte", ts),
        ])

        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)

        when:
        parser.getCurrentToken()
        parser.nextToken();

        then:
        ts == parser.getTimestampValue()
    }

    def 'should return javscript'() {
        given:
        def js = new BsonJavaScript("var a = 'this is some code'")
        BsonDocument doc = new BsonDocument([
                new BsonElement("byte", js),
        ])

        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)

        when:
        parser.getCurrentToken()
        parser.nextToken();

        then:
        js == parser.getJavascriptValue()
    }

    def 'should return date'() {
        given:
        def date = new Date()
        BsonDocument doc = new BsonDocument([
                new BsonElement("byte", new BsonDateTime(date.getTime())),
        ])

        def reader = new BsonDocumentReader(doc)
        def parser = new JacksonBsonParser(reader)

        when:
        parser.getCurrentToken()
        parser.nextToken();

        then:
        date == parser.getDateValue()
    }
}
