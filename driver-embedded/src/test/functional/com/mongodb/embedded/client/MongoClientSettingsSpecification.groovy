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

import com.mongodb.ConnectionString
import com.mongodb.event.CommandListener
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSettingsSpecification extends Specification {
    private static final String DB_PATH = '/path/to/db'

    def 'should require a dbPath'() {
        when:
        MongoClientSettings.builder().build()

        then:
        thrown(IllegalStateException)
    }

    def 'should set the correct default values'() {
        given:
        def settings = MongoClientSettings.builder().dbPath(DB_PATH).build()

        expect:
        settings.getDbPath() == DB_PATH
        settings.getCommandListeners().isEmpty()
        settings.getApplicationName() == null
    }

    @SuppressWarnings('UnnecessaryObjectReferences')
    def 'should handle illegal arguments'() {
        given:
        def builder = MongoClientSettings.builder()

        when:
        builder.codecRegistry(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.addCommandListener(null)
        then:
        thrown(IllegalArgumentException)
    }

    def 'should build with set configuration'() {
        given:
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)

        when:
        def settings = MongoClientSettings.builder()
                .dbPath(DB_PATH)
                .applicationName('app1')
                .addCommandListener(commandListener)
                .codecRegistry(codecRegistry)
                .libraryPath('/mongo/lib/')
                .build()

        then:
        settings.getDbPath() == DB_PATH
        settings.getApplicationName() == 'app1'
        settings.getCommandListeners() == [commandListener]
        settings.getCodecRegistry() == codecRegistry
        settings.getLibraryPath() == '/mongo/lib/'
    }

    def 'should be easy to create new settings from existing'() {
        when:
        def settings = MongoClientSettings.builder().dbPath(DB_PATH).build()

        then:
        expect settings, isTheSameAs(MongoClientSettings.builder(settings).build())

        when:
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)

        settings = MongoClientSettings.builder()
                .dbPath(DB_PATH)
                .applicationName('app1')
                .addCommandListener(commandListener)
                .codecRegistry(codecRegistry)
                .build()

        then:
        expect settings, isTheSameAs(MongoClientSettings.builder(settings).build())
    }

    def 'applicationName can be 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 126 + '\u00A0'

        when:
        def settings = MongoClientSettings.builder().dbPath(DB_PATH).applicationName(applicationName).build()

        then:
        settings.getApplicationName() == applicationName
    }

    def 'should throw IllegalArgumentException if applicationName exceeds 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 127 + '\u00A0'

        when:
        MongoClientSettings.builder().applicationName(applicationName)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should add command listeners'() {
        given:
        CommandListener commandListenerOne = Mock(CommandListener)
        CommandListener commandListenerTwo = Mock(CommandListener)
        CommandListener commandListenerThree = Mock(CommandListener)

        when:
        def settings = MongoClientSettings.builder().dbPath(DB_PATH).build()

        then:
        settings.getCommandListeners().size() == 0

        when:
        settings = MongoClientSettings.builder().dbPath(DB_PATH).addCommandListener(commandListenerOne).build()

        then:
        settings.getCommandListeners().size() == 1
        settings.getCommandListeners()[0].is commandListenerOne

        when:
        settings = MongoClientSettings.builder()
                .dbPath(DB_PATH)
                .addCommandListener(commandListenerOne)
                .addCommandListener(commandListenerTwo)
                .build()

        then:
        settings.getCommandListeners().size() == 2
        settings.getCommandListeners()[0].is commandListenerOne
        settings.getCommandListeners()[1].is commandListenerTwo

        when:
        def copied = MongoClientSettings.builder(settings).addCommandListener(commandListenerThree).build()

        then:
        copied.getCommandListeners().size() == 3
        copied.getCommandListeners()[0].is commandListenerOne
        copied.getCommandListeners()[1].is commandListenerTwo
        copied.getCommandListeners()[2].is commandListenerThree
        settings.getCommandListeners().size() == 2
        settings.getCommandListeners()[0].is commandListenerOne
        settings.getCommandListeners()[1].is commandListenerTwo
    }

    def 'should build settings from a connection string'() {
        when:
        ConnectionString connectionString = new ConnectionString('mongodb://%2Ftmp%2Fembedded/?appName=MyApp')
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connectionString).build()
        MongoClientSettings expected = MongoClientSettings.builder()
            .dbPath('/tmp/embedded')
            .applicationName('MyApp')
            .build()

        then:
        expect expected, isTheSameAs(settings, ['wrappedMongoClientSettings'])
    }

    def 'should only have the following fields in the builder'() {
        when:
        // A regression test so that if anymore fields are added then the builder(final MongoClientSettings settings) should be updated
        def actual = MongoClientSettings.Builder.declaredFields.grep {  !it.synthetic } *.name.sort()
        def expected = ['dbPath', 'libraryPath', 'wrappedBuilder']

        then:
        actual == expected
    }

    def 'should only have the following methods in the builder'() {
        when:
        // A regression test so that if anymore methods are added then the builder(final MongoClientSettings settings) should be updated
        def actual = MongoClientSettings.Builder.declaredMethods.grep {  !it.synthetic } *.name.sort()
        def expected = ['addCommandListener', 'applicationName', 'applyConnectionString', 'build', 'codecRegistry', 'commandListenerList',
                        'dbPath', 'libraryPath']
        then:
        actual == expected
    }
}
