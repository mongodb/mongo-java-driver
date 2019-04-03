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
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.MILLISECONDS


class SocketSettingsSpecification extends Specification {

    def 'should have correct defaults'() {
        when:
        def settings = SocketSettings.builder().build()

        then:
        settings.getConnectTimeout(MILLISECONDS) == 10000
        settings.getReadTimeout(MILLISECONDS) == 0
        settings.receiveBufferSize == 0
        settings.sendBufferSize == 0
    }

    def 'should set settings'() {
        when:
        def settings = SocketSettings.builder()
                                     .connectTimeout(5000, MILLISECONDS)
                                     .readTimeout(2000, MILLISECONDS)
                                     .sendBufferSize(1000)
                                     .receiveBufferSize(1500)
                                     .build()


        then:
        settings.getConnectTimeout(MILLISECONDS) == 5000
        settings.getReadTimeout(MILLISECONDS) == 2000
        settings.sendBufferSize == 1000
        settings.receiveBufferSize == 1500
    }

    def 'should apply builder settings'() {
        when:
        def original = SocketSettings.builder()
                .connectTimeout(5000, MILLISECONDS)
                .readTimeout(2000, MILLISECONDS)
                .sendBufferSize(1000)
                .receiveBufferSize(1500)
                .build()

        def settings = SocketSettings.builder(original).build()

        then:
        settings.getConnectTimeout(MILLISECONDS) == 5000
        settings.getReadTimeout(MILLISECONDS) == 2000
        settings.sendBufferSize == 1000
        settings.receiveBufferSize == 1500
    }

    def 'should apply connection string'() {
        when:
        def settings = SocketSettings.builder()
                                     .applyConnectionString(new ConnectionString
                                                                    ('mongodb://localhost/?connectTimeoutMS=5000&socketTimeoutMS=2000'))
                                     .build()


        then:
        settings.getConnectTimeout(MILLISECONDS) == 5000
        settings.getReadTimeout(MILLISECONDS) == 2000
        settings.sendBufferSize == 0
        settings.receiveBufferSize == 0
    }

    def 'should apply settings'() {
        given:
        def defaultSettings = SocketSettings.builder().build()
        def customSettings = SocketSettings.builder()
                .connectTimeout(5000, MILLISECONDS)
                .readTimeout(2000, MILLISECONDS)
                .sendBufferSize(1000)
                .receiveBufferSize(1500)
                .build()

        expect:
        SocketSettings.builder().applySettings(customSettings).build() == customSettings
        SocketSettings.builder(customSettings).applySettings(defaultSettings).build() == defaultSettings
    }

    def 'identical settings should be equal'() {
        expect:
        SocketSettings.builder().build() == SocketSettings.builder().build()
        SocketSettings.builder()
                      .connectTimeout(5000, MILLISECONDS)
                      .readTimeout(2000, MILLISECONDS)
                      .sendBufferSize(1000)
                      .receiveBufferSize(1500)
                      .build() ==
        SocketSettings.builder()
                      .connectTimeout(5000, MILLISECONDS)
                      .readTimeout(2000, MILLISECONDS)
                      .sendBufferSize(1000)
                      .receiveBufferSize(1500)
                      .build()
    }

    def 'different settings should not be equal'() {
        expect:
        SocketSettings.builder().receiveBufferSize(4) != SocketSettings.builder().receiveBufferSize(3).build()
    }

    def 'identical settings should have same hash code'() {
        expect:
        SocketSettings.builder().build().hashCode() == SocketSettings.builder().build().hashCode()
        SocketSettings.builder()
                      .connectTimeout(5000, MILLISECONDS)
                      .readTimeout(2000, MILLISECONDS)
                      .sendBufferSize(1000)
                      .receiveBufferSize(1500)
                      .build().hashCode() ==
        SocketSettings.builder()
                      .connectTimeout(5000, MILLISECONDS)
                      .readTimeout(2000, MILLISECONDS)
                      .sendBufferSize(1000)
                      .receiveBufferSize(1500)
                      .build().hashCode()
    }

    def 'different settings should have different hash codes'() {
        expect:
        SocketSettings.builder().sendBufferSize(4).build().hashCode() != SocketSettings.builder().sendBufferSize(3).build().hashCode()
    }
}
