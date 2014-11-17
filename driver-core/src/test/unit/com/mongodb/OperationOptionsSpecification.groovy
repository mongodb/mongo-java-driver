/*
 *
 *  * Copyright (c) 2008-2014 MongoDB, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.mongodb

import com.mongodb.client.options.OperationOptions
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

class OperationOptionsSpecification extends Specification {

    def 'should override inherited values'() {
        given:
        def options = OperationOptions.builder().codecRegistry(new RootCodecRegistry([]))
                                      .writeConcern(WriteConcern.ACKNOWLEDGED)
                                      .readPreference(ReadPreference.primary())
                                      .build()


        when:
        def customOptions = OperationOptions.builder().codecRegistry(new RootCodecRegistry([new ValueCodecProvider()])).build()
        def newOptions = customOptions.withDefaults(options)

        then:
        newOptions.getCodecRegistry() == customOptions.getCodecRegistry()
        newOptions.getReadPreference() == options.getReadPreference()
        newOptions.getWriteConcern() == options.getWriteConcern()

        when:
        customOptions = OperationOptions.builder().readPreference(ReadPreference.secondary()).build()
        newOptions = customOptions.withDefaults(options)

        then:
        newOptions.getCodecRegistry() == options.getCodecRegistry()
        newOptions.getReadPreference() == customOptions.getReadPreference()
        newOptions.getWriteConcern() == options.getWriteConcern()

        when:
        customOptions = OperationOptions.builder().writeConcern(WriteConcern.JOURNALED).build()
        newOptions = customOptions.withDefaults(options)

        then:
        newOptions.getCodecRegistry() == options.getCodecRegistry()
        newOptions.getReadPreference() == options.getReadPreference()
        newOptions.getWriteConcern() == customOptions.getWriteConcern()
    }

}
