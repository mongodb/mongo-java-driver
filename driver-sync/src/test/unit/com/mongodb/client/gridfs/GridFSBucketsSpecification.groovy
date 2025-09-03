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

package com.mongodb.client.gridfs

import com.mongodb.ClusterFixture
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.internal.MongoDatabaseImpl
import com.mongodb.client.internal.OperationExecutor
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static org.bson.UuidRepresentation.JAVA_LEGACY
import static spock.util.matcher.HamcrestSupport.expect

class GridFSBucketsSpecification extends Specification {

    def readConcern = ReadConcern.DEFAULT

    def 'should create a GridFSBucket with default bucket name'() {
        given:
        def database = new MongoDatabaseImpl('db', Stub(CodecRegistry), Stub(ReadPreference), Stub(WriteConcern), false, true, readConcern,
                JAVA_LEGACY, null, ClusterFixture.TIMEOUT_SETTINGS, Stub(OperationExecutor))

        when:
        def gridFSBucket = GridFSBuckets.create(database)

        then:
        expect gridFSBucket, isTheSameAs(new GridFSBucketImpl(database))
    }


    def 'should create a GridFSBucket with custom bucket name'() {
        given:
        def database = new MongoDatabaseImpl('db', Stub(CodecRegistry), Stub(ReadPreference), Stub(WriteConcern), false, true, readConcern,
                JAVA_LEGACY, null, ClusterFixture.TIMEOUT_SETTINGS, Stub(OperationExecutor))
        def customName = 'custom'

        when:
        def gridFSBucket = GridFSBuckets.create(database, customName)

        then:
        expect gridFSBucket, isTheSameAs(new GridFSBucketImpl(database, customName))
    }

}
