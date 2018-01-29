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

import org.bson.codecs.DateCodec
import org.bson.codecs.configuration.CodecRegistries
import org.bson.types.BSONTimestamp
import spock.lang.Specification

class DBObjectCodecProviderSpecification extends Specification {
    def provider = new DBObjectCodecProvider()
    def registry = CodecRegistries.fromProviders(provider)

    def 'should provide codec for BSONTimestamp'() {
        expect:
        provider.get(BSONTimestamp, registry).class == BSONTimestampCodec
    }

    def 'should provide codec for Date'() {
        expect:
        provider.get(Date, registry).class == DateCodec
    }

    def 'should provide codec for class assignable to DBObject'() {
        expect:
        provider.get(BasicDBObject, registry).class == DBObjectCodec
    }

    def 'should not provide codec for class assignable to DBObject that is also assignable to List'() {
        expect:
        provider.get(BasicDBList, registry) == null
    }

    def 'should not provide codec for unexpected class'() {
        expect:
        provider.get(Integer, registry) == null
    }
}
