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

import com.mongodb.ServerAddress
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import spock.lang.Specification
import spock.lang.Unroll

class AsynchronousSocketChannelStreamFactoryFactorySpecification extends Specification {

    @Unroll
    def 'should create the expected #description AsynchronousSocketChannelStream'() {
        given:
        def factory = new AsynchronousSocketChannelStreamFactoryFactory(new DefaultInetAddressResolver())
                .create(socketSettings, sslSettings)

        when:
        AsynchronousSocketChannelStream stream = factory.create(serverAddress) as AsynchronousSocketChannelStream

        then:
        stream.getSettings() == socketSettings
        stream.getAddress() == serverAddress
    }

    SocketSettings socketSettings = SocketSettings.builder().build()
    SslSettings sslSettings = SslSettings.builder().build()
    ServerAddress serverAddress = new ServerAddress()
}
