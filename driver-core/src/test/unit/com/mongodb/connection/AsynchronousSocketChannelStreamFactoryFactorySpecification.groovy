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

package com.mongodb.connection

import com.mongodb.ServerAddress
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static com.mongodb.ClusterFixture.isNotAtLeastJava7

@IgnoreIf({ isNotAtLeastJava7() })
class AsynchronousSocketChannelStreamFactoryFactorySpecification extends Specification {

    @Unroll
    def 'should create the expected #description AsynchronousSocketChannelStream'() {
        given:
        def factory = factoryFactory.create(socketSettings, sslSettings)

        when:
        AsynchronousSocketChannelStream stream = factory.create(serverAddress)

        then:
        stream.getSettings() == socketSettings
        stream.getAddress() == serverAddress
        (stream.getGroup() == null) == hasCustomGroup

        cleanup:
        stream.getGroup()?.shutdown()

        where:
        description | factoryFactory  | hasCustomGroup
        'default'   | DEFAULT_FACTORY | true
        'custom'    | CUSTOM_FACTORY  | false
    }

    SocketSettings socketSettings = SocketSettings.builder().build()
    SslSettings sslSettings = SslSettings.builder().build()
    ServerAddress serverAddress = new ServerAddress()
    ExecutorService service = Executors.newFixedThreadPool(1)
    static final DEFAULT_FACTORY = AsynchronousSocketChannelStreamFactoryFactory.builder().build()
    static final CUSTOM_FACTORY = AsynchronousSocketChannelStreamFactoryFactory.builder()
            .group(java.nio.channels.AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(5)))
            .build()
}
