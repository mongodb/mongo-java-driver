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

import spock.lang.Specification

class MongoDriverInformationSpecification extends Specification {

    def 'should set the correct default values'() {
        given:
        def options = MongoDriverInformation.builder().build()

        expect:
        options.getDriverNames() ==  []
        options.getDriverVersions() ==  []
        options.getDriverPlatforms() ==  []
    }

    def 'should not prepend data if none has been added'() {
        given:
        def options = MongoDriverInformation.builder(MongoDriverInformation.builder().build()).build()

        expect:
        options.getDriverNames() ==  []
        options.getDriverVersions() ==  []
        options.getDriverPlatforms() ==  []
    }

    def 'should prepend data to the list'() {
        given:
        def scalaDriverInfo = MongoDriverInformation.builder()
                .driverName('mongo-scala-driver')
                .driverVersion('1.2.0')
                .driverPlatform('Scala 2.11')
                .build()

        def options = MongoDriverInformation.builder(scalaDriverInfo)
                .driverName('mongo-java-driver')
                .driverVersion('3.4.0')
                .driverPlatform('Java oracle64-1.8.0.31')
                .build()

        expect:
        options.getDriverNames() == ['mongo-java-driver', 'mongo-scala-driver']
        options.getDriverVersions() == ['3.4.0', '1.2.0']
        options.getDriverPlatforms() == ['Java oracle64-1.8.0.31', 'Scala 2.11']
    }

    def 'should only prepend data that has been set'() {
        given:
        def scalaDriverInfo = MongoDriverInformation.builder().driverName('mongo-scala-driver').build()

        def options = MongoDriverInformation.builder(scalaDriverInfo)
                .driverName('mongo-java-driver')
                .driverVersion('3.4.0')
                .driverPlatform('Java oracle64-1.8.0.31')
                .build()

        expect:
        options.getDriverNames() == ['mongo-java-driver', 'mongo-scala-driver']
        options.getDriverVersions() == ['3.4.0']
        options.getDriverPlatforms() == ['Java oracle64-1.8.0.31']
    }

    def 'should error if trying to set a version without setting a name'() {
        when:
        MongoDriverInformation.builder().driverVersion('0.21.1-alpha').build()

        then:
        thrown IllegalStateException
    }

    def 'should null check the passed MongoDriverInformation'() {
        when:
        MongoDriverInformation.builder(null).build()

        then:
        thrown IllegalArgumentException
    }
}
