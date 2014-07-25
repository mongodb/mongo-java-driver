/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import spock.lang.Specification
import spock.lang.Unroll

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class ConnectionPoolSettingsSpecification extends Specification {
    @Unroll
    def 'should set up connection provider settings #settings correctly'() {
        expect:
        settings.getMaxWaitTime(MILLISECONDS) == maxWaitTime
        settings.maxSize == maxSize
        settings.maxWaitQueueSize == maxWaitQueueSize
        settings.getMaxConnectionLifeTime(MILLISECONDS) == maxConnectionLifeTimeMS
        settings.getMaxConnectionIdleTime(MILLISECONDS) == maxConnectionIdleTimeMS
        settings.minSize == minSize
        settings.getMaintenanceInitialDelay(MILLISECONDS) == maintenanceInitialDelayMS
        settings.getMaintenanceFrequency(MILLISECONDS) == maintenanceFrequencyMS

        where:
        settings                              | maxWaitTime | maxSize | maxWaitQueueSize | maxConnectionLifeTimeMS |
                maxConnectionIdleTimeMS | minSize | maintenanceInitialDelayMS | maintenanceFrequencyMS
        ConnectionPoolSettings
                .builder()
                .build()                       | 120000L    | 100  | 500  |      0 |     0 | 0 | 0 | 60000
        ConnectionPoolSettings
                .builder()
                .maxWaitTime(5, SECONDS)
                .maxSize(75)
                .maxWaitQueueSize(11)
                .maxConnectionLifeTime(
                101, SECONDS)
                .maxConnectionIdleTime(
                51, SECONDS)
                .minSize(1)
                .maintenanceInitialDelay(
                5, SECONDS)
                .maintenanceFrequency(
                1000, SECONDS)
                .build()                      | 5000 | 75 | 11 | 101000 | 51000 | 1 | 5000 | 1000000
    }

    def 'should throw exception on invalid argument'() {
        when:
        ConnectionPoolSettings.builder().maxSize(1).maxWaitQueueSize(-1).build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionPoolSettings.builder().maxSize(1).maxConnectionLifeTime(-1, SECONDS).build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionPoolSettings.builder().maxSize(1).maxConnectionIdleTime(-1, SECONDS).build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionPoolSettings.builder().maxSize(1).minSize(2).build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionPoolSettings.builder().maintenanceInitialDelay(-1, MILLISECONDS).build()

        then:
        thrown(IllegalStateException)

        when:
        ConnectionPoolSettings.builder().maintenanceFrequency(0, MILLISECONDS).build()

        then:
        thrown(IllegalStateException)
    }

    def 'settings with same values should be equal'() {
        when:
        def settings1 = ConnectionPoolSettings.builder().maxSize(1).build()
        def settings2 = ConnectionPoolSettings.builder().maxSize(1).build()

        then:
        settings1 == settings2
    }

    def 'settings with same values should have the same hash code'() {
        when:
        def settings1 = ConnectionPoolSettings.builder().maxSize(1).build()
        def settings2 = ConnectionPoolSettings.builder().maxSize(1).build()

        then:
        settings1.hashCode() == settings2.hashCode()
    }

    def 'toString should be overridden'() {
        when:
        def settings = ConnectionPoolSettings.builder().maxSize(1).build()

        then:
        settings.toString().startsWith('ConnectionPoolSettings')
    }
}
