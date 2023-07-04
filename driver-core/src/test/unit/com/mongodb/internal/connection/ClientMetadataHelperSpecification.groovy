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

package com.mongodb.internal.connection

import com.mongodb.MongoDriverInformation
import com.mongodb.internal.build.MongoDriverVersion
import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.EncoderContext
import org.bson.io.BasicOutputBuffer
import spock.lang.Specification

import static com.mongodb.internal.connection.ClientMetadataHelper.getClientMetadataDocument

// See also: ClientMetadataHelperProseTest.java
class ClientMetadataHelperSpecification extends Specification {

    def 'applicationName can be 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 126 + '\u00A0'

        when:
        def clientMetadataDocument = getClientMetadataDocument(applicationName, null)

        then:
        clientMetadataDocument.getDocument('application').getString('name').getValue() == applicationName
    }

    def 'should throw IllegalArgumentException if applicationName exceeds 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 127 + '\u00A0'

        when:
        getClientMetadataDocument(applicationName, null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create client metadata document'() {
        expect:
        getClientMetadataDocument(applicationName, null) == createExpectedClientMetadataDocument(applicationName)

        where:
        applicationName << ['appName', null]
    }

    def 'should create client metadata document including driver info'() {
        given:
        def applicationName = 'appName'
        def driverInfo = createDriverInformation();

        expect:
        getClientMetadataDocument(applicationName, driverInfo) == createExpectedClientMetadataDocument(applicationName, driverInfo)
    }

    def 'should get operating system type from name'() {
        expect:
        ClientMetadataHelper.getOperatingSystemType(name) == type

        where:
        name            | type
        'unknown'       | 'unknown'
        'Linux OS'      | 'Linux'
        'Mac OS X'      | 'Darwin'
        'Windows 10'    | 'Windows'
        'HP-UX OS'      | 'Unix'
        'AIX OS'        | 'Unix'
        'Irix OS'       | 'Unix'
        'Solaris OS'    | 'Unix'
        'SunOS'         | 'Unix'
        'Some Other OS' | 'unknown'
    }

    def encode(final BsonDocument document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer()
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build())
        buffer.toByteArray()
    }

    static MongoDriverInformation createDriverInformation() {
        MongoDriverInformation.builder()
                .driverName('mongo-spark')
                .driverVersion('2.0.0')
                .driverPlatform('Scala 2.10 / Spark 2.0.0')
                .build()
    }

    static BsonDocument createExpectedClientMetadataDocument(String appName) {
        def expectedDriverDocument = new BsonDocument('name', new BsonString(MongoDriverVersion.NAME))
                .append('version', new BsonString(MongoDriverVersion.VERSION))
        def expectedOperatingSystemDocument = new BsonDocument('type',
                new BsonString(ClientMetadataHelper.getOperatingSystemType(System.getProperty('os.name'))))
                .append('name', new BsonString(System.getProperty('os.name')))
                .append('architecture', new BsonString(System.getProperty('os.arch')))
                .append('version', new BsonString(System.getProperty('os.version')))
        def expectedClientDocument = new BsonDocument()
        if (appName != null) {
            expectedClientDocument.append('application', new BsonDocument('name', new BsonString(appName)))
        }
        expectedClientDocument
                .append('driver', expectedDriverDocument)
                .append('os', expectedOperatingSystemDocument)
                .append('platform', new BsonString('Java/' + System.getProperty('java.vendor') + '/'
                        + System.getProperty('java.runtime.version')))

        expectedClientDocument
    }

    static BsonDocument createExpectedClientMetadataDocument(String appName, MongoDriverInformation driverInformation) {
        String separator = '|'
        def expectedClientDocument = createExpectedClientMetadataDocument(appName).clone()

        def expectedDriverDocument = expectedClientDocument.getDocument('driver')
        def names = [expectedDriverDocument.getString('name').getValue(), *driverInformation.getDriverNames()].join(separator)
        def versions = [expectedDriverDocument.getString('version').getValue(), *driverInformation.getDriverVersions()].join(separator)

        expectedDriverDocument.append('name', new BsonString(names))
        expectedDriverDocument.append('version', new BsonString(versions))

        def platforms = [expectedClientDocument.getString('platform').getValue(), *driverInformation.getDriverPlatforms()].join(separator)
        expectedClientDocument.append('platform', new BsonString(platforms))
        expectedClientDocument
    }
}
