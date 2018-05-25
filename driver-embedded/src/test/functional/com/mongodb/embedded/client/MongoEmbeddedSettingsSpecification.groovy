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

package com.mongodb.embedded.client

import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoEmbeddedSettingsSpecification extends Specification {

    def 'should be easy to create new settings from existing'() {
        when:
        def settings = MongoEmbeddedSettings.builder().build()

        then:
        expect settings, isTheSameAs(MongoEmbeddedSettings.builder(settings).build())
    }

    def 'should only have the following methods in the builder'() {
        when:
        // A regression test so that if anymore methods are added then the builder(final MongoEmbeddedSettings settings) should be updated
        def actual = MongoEmbeddedSettings.Builder.declaredMethods.grep {  !it.synthetic } *.name.sort()
        def expected = ['build']

        then:
        actual == expected
    }
}
