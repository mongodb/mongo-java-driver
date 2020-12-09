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

package com.mongodb.internal.binding

import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.connection.ServerDescription
import com.mongodb.internal.connection.AsyncConnection
import spock.lang.Specification

import static com.mongodb.connection.ServerConnectionState.CONNECTED

class AsyncSingleConnectionReadBindingSpecification extends Specification {
    private final ServerDescription serverDescription = ServerDescription.builder()
                                                                         .address(new ServerAddress())
                                                                         .state(CONNECTED)
                                                                         .build()

    def 'binding should get read preference'() {
        given:
        def connection = Stub(AsyncConnection)

        when:
        def binding = new AsyncSingleConnectionReadBinding(ReadPreference.secondary(),
                serverDescription,
                connection, null)

        then:
        binding.getReadPreference() == ReadPreference.secondary()
    }

    def 'binding should retain and release self and connection'() {
        given:
        def connection = Mock(AsyncConnection)

        when:
        def binding = new AsyncSingleConnectionReadBinding(ReadPreference.primary(),
                serverDescription,
                connection, null)

        then:
        binding.getCount() == 1
        1 * connection.retain() >> connection

        when:
        binding.retain()

        then:
        binding.getCount() == 2

        when:
        binding.release()

        then:
        0 * connection.release()

        when:
        binding.release()

        then:
        1 * connection.release()
    }

    def 'connection source should get server description'() {
        given:
        def connection = Stub(AsyncConnection)
        def binding = new AsyncSingleConnectionReadBinding(ReadPreference.primary(),
                serverDescription,
                connection, null)

        when:
        AsyncConnectionSource source
        binding.getReadConnectionSource { AsyncConnectionSource result, Throwable t ->
            source = result
        }

        then:
        source.serverDescription == serverDescription
    }

    def 'connection source should retain and release self and connection'() {
        given:
        def connection = Mock(AsyncConnection)

        when:
        def binding = new AsyncSingleConnectionReadBinding(ReadPreference.primary(),
                serverDescription,
                connection, null)

        then:
        1 * connection.retain() >> connection

        when:
        AsyncConnectionSource source
        binding.getReadConnectionSource { AsyncConnectionSource result, Throwable t ->
            source = result
        }

        then:
        source.count == 1
        binding.count == 2

        when:
        source.retain()
        binding.count == 2

        then:
        source.count == 2

        when:
        source.getConnection { AsyncConnection result, Throwable t ->
        }

        then:
        1 * connection.retain() >> connection

        when:
        source.release()
        binding.count == 2

        then:
        source.count == 1

        when:
        source.release()

        then:
        source.count == 0
        binding.count == 1
    }
}
