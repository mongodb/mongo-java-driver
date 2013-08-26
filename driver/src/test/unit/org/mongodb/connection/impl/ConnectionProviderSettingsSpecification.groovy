/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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











package org.mongodb.connection.impl

import spock.lang.Specification
import spock.lang.Unroll

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class ConnectionProviderSettingsSpecification extends Specification {
    @Unroll
    def 'should set up connection provider settings #settings correctly'() {
        expect:
        settings.getMaxWaitTime(MILLISECONDS) == maxWaitTime
        settings.maxSize == maxSize
        settings.maxWaitQueueSize == maxWaitQueueSize
        settings.getMaxConnectionLifeTime(MILLISECONDS) == maxConnectionLifeTimeMS
        settings.getMaxConnectionIdleTime(MILLISECONDS) == maxConnectionIdleTimeMS
        settings.minSize == minSize
        settings.getMaintenanceFrequency(MILLISECONDS) == maintancanceFrequencyMS

        where:
        settings                              | maxWaitTime | maxSize | maxWaitQueueSize | maxConnectionLifeTimeMS |
                maxConnectionIdleTimeMS | minSize | maintancanceFrequencyMS
        ConnectionProviderSettings
                .builder()
                .maxSize(1).build()           | 0L   | 1  | 0  |      0 |     0 | 0 | 60000
        ConnectionProviderSettings
                .builder()
                .maxWaitTime(5, SECONDS)
                .maxSize(75)
                .maxWaitQueueSize(11)
                .maxConnectionLifeTime(
                101, SECONDS)
                .maxConnectionIdleTime(
                51, SECONDS)
                .minSize(1)
                .maintenanceFrequency(
                1000, SECONDS)
                .build()                      | 5000 | 75 | 11 | 101000 | 51000 | 1 | 1000000
    }

    def 'should throw exception on invalid argument'() {
        when:
        ConnectionProviderSettings.builder().build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(-1).build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionProviderSettings.builder().maxSize(1).maxConnectionLifeTime(-1, SECONDS).build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionProviderSettings.builder().maxSize(1).maxConnectionIdleTime(-1, SECONDS).build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionProviderSettings.builder().maxSize(1).minSize(2).build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionProviderSettings.builder().maintenanceFrequency(0, MILLISECONDS).build()

        then:
        thrown(IllegalStateException)
    }

    def 'settings with same values should be equal'() {
        when:
        def settings1 = ConnectionProviderSettings.builder().maxSize(1).build()
        def settings2 = ConnectionProviderSettings.builder().maxSize(1).build()

        then:
        settings1 == settings2
    }

    def 'settings with same values should have the same hash code'() {
        when:
        def settings1 = ConnectionProviderSettings.builder().maxSize(1).build()
        def settings2 = ConnectionProviderSettings.builder().maxSize(1).build()

        then:
        settings1.hashCode() == settings2.hashCode()
    }

    def 'toString should be overridden'() {
        when:
        def settings = ConnectionProviderSettings.builder().maxSize(1).build()

        then:
        settings.toString().startsWith('ConnectionProviderSettings')
    }
}
