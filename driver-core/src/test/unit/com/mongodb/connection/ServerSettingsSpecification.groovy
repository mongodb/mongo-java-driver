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

import com.mongodb.ConnectionString
import com.mongodb.event.ServerListener
import com.mongodb.event.ServerMonitorListener
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

/**
 * Update {@link ServerSettingsTest} instead.
 */
class ServerSettingsSpecification extends Specification {
    def 'should have correct defaults'() {
        when:
        def settings = ServerSettings.builder().build()

        then:
        settings.getHeartbeatFrequency(MILLISECONDS) == 10000
        settings.getMinHeartbeatFrequency(MILLISECONDS) == 500
        settings.serverListeners == []
        settings.serverMonitorListeners == []
    }

    def 'should apply builder settings'() {
        given:
        def serverListenerOne = new ServerListener() { }
        def serverListenerTwo = new ServerListener() { }
        def serverListenerThree = new ServerListener() { }
        def serverMonitorListenerOne = new ServerMonitorListener() { }
        def serverMonitorListenerTwo = new ServerMonitorListener() { }
        def serverMonitorListenerThree = new ServerMonitorListener() { }

        when:
        def settings = ServerSettings.builder()
                .heartbeatFrequency(4, SECONDS)
                .minHeartbeatFrequency(1, SECONDS)
                .addServerListener(serverListenerOne)
                .addServerListener(serverListenerTwo)
                .addServerMonitorListener(serverMonitorListenerOne)
                .addServerMonitorListener(serverMonitorListenerTwo)
                .build()


        then:
        settings.getHeartbeatFrequency(MILLISECONDS) == 4000
        settings.getMinHeartbeatFrequency(MILLISECONDS) == 1000
        settings.serverListeners == [serverListenerOne, serverListenerTwo]
        settings.serverMonitorListeners == [serverMonitorListenerOne, serverMonitorListenerTwo]

        when:
        settings = ServerSettings.builder()
                .serverListenerList([serverListenerThree])
                .serverMonitorListenerList([serverMonitorListenerThree]).build()

        then:
        settings.serverListeners == [serverListenerThree]
        settings.serverMonitorListeners == [serverMonitorListenerThree]
    }

    def 'when connection string is applied to builder, all properties should be set'() {
        when:
        def settings = ServerSettings.builder().applyConnectionString(new ConnectionString('mongodb://example.com:27018/?' +
                'heartbeatFrequencyMS=20000'))
                .build()

        then:
        settings.getHeartbeatFrequency(MILLISECONDS) == 20000
    }

    def 'should apply settings'() {
        given:
        def serverListenerOne = new ServerListener() { }
        def serverMonitorListenerOne = new ServerMonitorListener() { }
        def defaultSettings = ServerSettings.builder().build()
        def customSettings = ServerSettings.builder()
                .heartbeatFrequency(4, SECONDS)
                .minHeartbeatFrequency(1, SECONDS)
                .addServerListener(serverListenerOne)
                .addServerMonitorListener(serverMonitorListenerOne)
                .build()

        expect:
        ServerSettings.builder().applySettings(customSettings).build() == customSettings
        ServerSettings.builder(customSettings).applySettings(defaultSettings).build() == defaultSettings
    }

    def 'lists of listeners should be unmodifiable'() {
        given:
        def settings = ServerSettings.builder().build()

        when:
        settings.serverListeners.add(new ServerListener() { })

        then:
        thrown(UnsupportedOperationException)

        when:
        settings.serverMonitorListeners.add(new ServerMonitorListener() { })

        then:
        thrown(UnsupportedOperationException)
    }

    def 'listeners should not be null'() {
        when:
        ServerSettings.builder().addServerListener(null)

        then:
        thrown(IllegalArgumentException)

        when:
        ServerSettings.builder().addServerMonitorListener(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'identical settings should be equal'() {
        given:
        def serverListenerOne = new ServerListener() { }
        def serverMonitorListenerOne = new ServerMonitorListener() { }

        expect:
        ServerSettings.builder().build() == ServerSettings.builder().build()
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .addServerListener(serverListenerOne)
                      .addServerMonitorListener(serverMonitorListenerOne)
                      .build() ==
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .addServerListener(serverListenerOne)
                      .addServerMonitorListener(serverMonitorListenerOne)
                      .build()
    }

    def 'different settings should not be equal'() {
        expect:
        ServerSettings.builder().heartbeatFrequency(4, SECONDS).build() != ServerSettings.builder().heartbeatFrequency(3, SECONDS).build()
    }

    def 'identical settings should have same hash code'() {
        given:
        def serverListenerOne = new ServerListener() { }
        def serverMonitorListenerOne = new ServerMonitorListener() { }

        expect:
        ServerSettings.builder().build().hashCode() == ServerSettings.builder().build().hashCode()
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .addServerListener(serverListenerOne)
                      .addServerMonitorListener(serverMonitorListenerOne)
                      .build().hashCode() ==
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .addServerListener(serverListenerOne)
                      .addServerMonitorListener(serverMonitorListenerOne)
                      .build().hashCode()
    }

    def 'different settings should have different hash codes'() {
        expect:
        ServerSettings.builder().heartbeatFrequency(4, SECONDS).build().hashCode() !=
        ServerSettings.builder().heartbeatFrequency(3, SECONDS).build().hashCode()
    }
}
