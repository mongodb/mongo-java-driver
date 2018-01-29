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

package com.mongodb.event

import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ServerId
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

class CommandEventMulticasterSpecification extends Specification {

    def 'should get listeners'() {
        given:
        def first = Mock(CommandListener)
        def second = Mock(CommandListener)
        def multicaster = new CommandEventMulticaster([first, second])

        expect:
        multicaster.commandListeners == [first, second]
    }

    def 'should multicast command started event'() {
        given:
        def first = Mock(CommandListener)
        def second = Mock(CommandListener)
        def multicaster = new CommandEventMulticaster([first, second])
        def event = new CommandStartedEvent(1, new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress())),
                                            'admin', 'ping', new BsonDocument('ping', new BsonInt32(1)))

        when:
        multicaster.commandStarted(event)

        then:
        1 * first.commandStarted(event)
        1 * second.commandStarted(event)
    }

    def 'should continue multicasting command started event when a listener throws an Exception'() {
        given:
        def first = Stub(CommandListener) {
            commandStarted(_) >> {
                throw new UnsupportedOperationException()
            }
        }
        def second = Mock(CommandListener)
        def multicaster = new CommandEventMulticaster([first, second])
        def event = new CommandStartedEvent(1, new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress())),
                                            'admin', 'ping', new BsonDocument('ping', new BsonInt32(1)))

        when:
        multicaster.commandStarted(event)

        then:
        1 * second.commandStarted(event)
    }

    def 'should multicast command succeeded event'() {
        given:
        def first = Mock(CommandListener)
        def second = Mock(CommandListener)
        def multicaster = new CommandEventMulticaster([first, second])
        def event = new CommandSucceededEvent(1, new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress())),
                                              'ping', new BsonDocument('ok', new BsonInt32(1)), 1000)

        when:
        multicaster.commandSucceeded(event)

        then:
        1 * first.commandSucceeded(event)
        1 * second.commandSucceeded(event)
    }

    def 'should continue multicasting command succeeded event when a listener throws an Exception'() {
        given:
        def first = Stub(CommandListener) {
            commandSucceeded(_) >> {
                throw new UnsupportedOperationException()
            }
        }
        def second = Mock(CommandListener)
        def multicaster = new CommandEventMulticaster([first, second])
        def event = new CommandSucceededEvent(1, new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress())),
                                              'ping', new BsonDocument('ok', new BsonInt32(1)), 1000)

        when:
        multicaster.commandSucceeded(event)

        then:
        1 * second.commandSucceeded(event)
    }

    def 'should multicast command failed event'() {
        given:
        def first = Mock(CommandListener)
        def second = Mock(CommandListener)
        def multicaster = new CommandEventMulticaster([first, second])
        def event = new CommandFailedEvent(1, new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress())),
                                           'ping', 1000, new NullPointerException())

        when:
        multicaster.commandFailed(event)

        then:
        1 * first.commandFailed(event)
        1 * second.commandFailed(event)
    }

    def 'should continue multicasting command failed event when a listener throws an Exception'() {
        given:
        def first = Stub(CommandListener) {
            commandFailed(_) >> {
                throw new UnsupportedOperationException()
            }
        }
        def second = Mock(CommandListener)
        def multicaster = new CommandEventMulticaster([first, second])
        def event = new CommandFailedEvent(1, new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress())),
                                           'ping', 1000, new NullPointerException())

        when:
        multicaster.commandFailed(event)

        then:
        1 * second.commandFailed(event)
    }
}
