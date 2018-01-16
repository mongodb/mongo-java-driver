/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.gridfs.codecs

import com.mongodb.client.gridfs.model.GridFSFile
import org.bson.BsonBinaryReader
import org.bson.BsonBinaryWriter
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.BsonType
import org.bson.ByteBufNIO
import org.bson.Document
import org.bson.codecs.BsonTypeClassMap
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.Codec
import org.bson.codecs.DecoderContext
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.EncoderContext
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecRegistry
import org.bson.io.BasicOutputBuffer
import org.bson.io.ByteBufferBsonInput
import org.bson.types.ObjectId
import spock.lang.Specification

import java.nio.ByteBuffer

import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries

class GridFSFileCodecSpecification extends Specification {

    static final REGISTRY = fromProviders([new BsonValueCodecProvider(), new ValueCodecProvider(), new DocumentCodecProvider()])
    static final BSONTYPESREGISTRY = fromRegistries(
            fromProviders(new DocumentCodecProvider(new BsonTypeClassMap([(BsonType.STRING): BsonString]))), REGISTRY)
    static final CODEC = new GridFSFileCodec(REGISTRY)
    static final ID = new BsonObjectId(new ObjectId())
    static final FILENAME = 'filename'
    static final LENGTH = 100L
    static final CHUNKSIZE = 255
    static final UPLOADDATE = new Date()
    static final MD5 = '000000'
    static final METADATA = new Document('field', 'value')
    static final EXTRAELEMENTS = new Document('contentType', 'text/txt').append('aliases', ['fileb'])

    def 'should encode and decode all default types with all readers and writers'() {
        expect:
        roundTripped(original) == original

        where:
        original << [
            new GridFSFile(ID, FILENAME, LENGTH, CHUNKSIZE, UPLOADDATE, MD5, null, null),
            new GridFSFile(ID, FILENAME, LENGTH, CHUNKSIZE, UPLOADDATE, MD5, METADATA, null),
            new GridFSFile(ID, FILENAME, LENGTH, CHUNKSIZE, UPLOADDATE, MD5, null, EXTRAELEMENTS),
            new GridFSFile(ID, FILENAME, LENGTH, CHUNKSIZE, UPLOADDATE, MD5, METADATA, EXTRAELEMENTS)
        ]
    }

    def 'it should decode extra elements'() {
        given:
        def expected = new GridFSFile(ID, FILENAME, LENGTH, CHUNKSIZE, UPLOADDATE, MD5, METADATA, EXTRAELEMENTS)

        when:
        def gridFSFileFromDocument = toGridFSFile(['_id': ID, 'filename': FILENAME, 'length': LENGTH, 'chunkSize': CHUNKSIZE,
                                              'uploadDate': UPLOADDATE, 'md5': MD5, 'metadata': METADATA,
                                              'contentType': 'text/txt', 'aliases': ['fileb']] as Document)

        then:
        gridFSFileFromDocument == expected
    }

    def 'it should use the users codec for metadata / extra elements'() {
        when:
        def gridFSFileFromDocument = toGridFSFile(['_id': ID, 'filename': FILENAME, 'length': LENGTH, 'chunkSize': CHUNKSIZE,
                                                   'uploadDate': UPLOADDATE, 'md5': MD5, 'metadata': METADATA] as Document,
                BSONTYPESREGISTRY)
        then:
        gridFSFileFromDocument.metadata.get('field') == new BsonString('value')
    }

    GridFSFile roundTripped(GridFSFile gridFSFile) {
        def writer = new BsonBinaryWriter(new BasicOutputBuffer())
        encode(writer, gridFSFile, CODEC)
        decode(writer, CODEC)
    }

    GridFSFile toGridFSFile(Document document) {
        toGridFSFile(document, REGISTRY)
    }

    GridFSFile toGridFSFile(Document document, CodecRegistry registry) {
        def writer = new BsonBinaryWriter(new BasicOutputBuffer())
        registry.get(Document).encode(writer, document, EncoderContext.builder().build())
        decode(writer, new GridFSFileCodec(registry))
    }

    def encode(BsonBinaryWriter writer, GridFSFile gridFSFile, Codec<GridFSFile> codec) {
        codec.encode(writer, gridFSFile, EncoderContext.builder().build())
    }

    def <T> T decode(BsonBinaryWriter writer, Codec<T> codec) {
        def reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(writer.bsonOutput.toByteArray()))))
        codec.decode(reader, DecoderContext.builder().build())
    }
}
