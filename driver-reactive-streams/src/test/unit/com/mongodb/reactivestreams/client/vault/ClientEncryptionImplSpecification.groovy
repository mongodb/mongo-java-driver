/*
 * Copyright 2018 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.vault


import com.mongodb.client.model.vault.DataKeyOptions
import com.mongodb.client.model.vault.EncryptOptions
import org.bson.BsonBinary
import org.reactivestreams.Subscriber
import spock.lang.Specification

class ClientEncryptionImplSpecification extends Specification {

    def 'should forward methods to wrapped'() {
        given:
        def wrapped = Mock(com.mongodb.async.client.vault.ClientEncryption)
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { it[0].request(1) }
        }
        def kmsProvider = 'test'
        def dataKeyOptions = new DataKeyOptions()
        def bsonBinary = Stub(BsonBinary)
        def encryptOptions = new EncryptOptions()
        def clientEncryption = new ClientEncryptionImpl(wrapped)

        when:
        clientEncryption.createDataKey(kmsProvider).subscribe(subscriber)

        then:
        1 * wrapped.createDataKey(kmsProvider, _, _)

        when:

        clientEncryption.createDataKey(kmsProvider, dataKeyOptions).subscribe(subscriber)

        then:
        1 * wrapped.createDataKey(kmsProvider, dataKeyOptions, _)

        when:
        clientEncryption.encrypt(bsonBinary, encryptOptions).subscribe(subscriber)

        then:
        1 * wrapped.encrypt(bsonBinary, encryptOptions, _)

        when:
        clientEncryption.decrypt(bsonBinary).subscribe(subscriber)

        then:
        1 * wrapped.decrypt(bsonBinary, _)

        when:
        clientEncryption.close()

        then:
        1 * wrapped.close()
    }
}
