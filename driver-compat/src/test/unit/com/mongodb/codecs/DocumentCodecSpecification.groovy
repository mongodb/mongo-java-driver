package com.mongodb.codecs

import com.mongodb.DBRef
import org.bson.BSONWriter
import org.bson.BasicBSONObject
import org.bson.types.CodeWScope
import org.bson.types.ObjectId
import org.mongodb.Document
import org.mongodb.codecs.PrimitiveCodecs
import spock.lang.Specification

class DocumentCodecSpecification extends Specification {

    def 'should encode driver-compat com.mongodb.DBRef type'() {
        given:
        ObjectId objectId = new ObjectId()
        Document document = new Document('a', new DBRef(null, 'foo', objectId))
        BSONWriter bsonWriter = Mock()

        when:
        new DocumentCodec(PrimitiveCodecs.createDefault()).encode(bsonWriter, document)

        then:
        1 * bsonWriter.writeStartDocument()
        1 * bsonWriter.writeName('a')
        1 * bsonWriter.writeStartDocument()
        1 * bsonWriter.writeString('$ref', 'foo')
        1 * bsonWriter.writeName('$id')
        1 * bsonWriter.writeObjectId(objectId)
        2 * bsonWriter.writeEndDocument()
    }

    def 'should encode driver-compat CodeWScope type'() {
        given:
        Document document = new Document('c', new CodeWScope('i++', new BasicBSONObject('i', 0)))
        BSONWriter bsonWriter = Mock()

        when:
        new DocumentCodec(PrimitiveCodecs.createDefault()).encode(bsonWriter, document)

        then:
        1 * bsonWriter.writeStartDocument()
        1 * bsonWriter.writeName('c')
        1 * bsonWriter.writeJavaScriptWithScope('i++')
        1 * bsonWriter.writeStartDocument()
        1 * bsonWriter.writeName('i')
        1 * bsonWriter.writeInt32(0)
        2 * bsonWriter.writeEndDocument()
    }
}
