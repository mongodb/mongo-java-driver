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
import com.mongodb.MongoInternalException
import spock.lang.IgnoreIf
import spock.lang.Specification

import javax.net.ssl.SSLContext

import static com.mongodb.ClusterFixture.isNotAtLeastJava7

class SslSettingsSpecification extends Specification {

    def 'should have the expected defaults'() {
        when:
        def settings = SslSettings.builder().build()

        then:
        settings.context == null
        !settings.enabled
        !settings.invalidHostNameAllowed
    }

    @IgnoreIf({ isNotAtLeastJava7() })
    def 'should set settings'() {
        when:
        def settings = SslSettings.builder()
                .context(SSLContext.getDefault())
                .enabled(true)
                .invalidHostNameAllowed(true)
                .build()

        then:
        settings.context == SSLContext.getDefault()
        settings.enabled
        settings.invalidHostNameAllowed
    }

    def 'should not allow invalid host name on Java 6'() {
        given:
        String javaVersion = System.getProperty('java.version')

        when:
        System.setProperty('java.version', '1.6.0_45')
        SslSettings.builder().enabled(true).build()

        then:
        def e = thrown(MongoInternalException)
        e.message.contains('SslSettings.invalidHostNameAllowed')

        cleanup:
        System.setProperty('java.version', javaVersion)
    }

    @IgnoreIf({ isNotAtLeastJava7() })
    def 'should apply connection string without ssl'() {
        expect:
        builder.applyConnectionString(new ConnectionString(connectionString)).build() == expected

        where:
        connectionString                        | builder                               | expected
        'mongodb://localhost'                   | SslSettings.builder()                 | SslSettings.builder().build()
        'mongodb://localhost/?ssl=false'        | SslSettings.builder()                 | SslSettings.builder().build()
        'mongodb://localhost/?ssl=true'         | SslSettings.builder()                 | SslSettings.builder().enabled(true).build()
        'mongodb://localhost/?ssl=true' +
            '&sslInvalidHostNameAllowed=true'   | SslSettings.builder()                 | SslSettings.builder().enabled(true)
                                                                                            .invalidHostNameAllowed(true).build()
        'mongodb://localhost/?ssl=true' +
            '&sslInvalidHostNameAllowed=true'   | SslSettings.builder()
                                                    .context(SSLContext.getDefault())    | SslSettings.builder().enabled(true)
                                                                                                .context(SSLContext.getDefault())
                                                                                                .invalidHostNameAllowed(true).build()
    }

    @IgnoreIf({ isNotAtLeastJava7() })
    def 'should apply settings'() {
        given:
        def defaultSettings = SslSettings.builder().build()
        def customSettings = SslSettings.builder()
                .context(SSLContext.getDefault())
                .enabled(true)
                .invalidHostNameAllowed(true)
                .build()

        expect:
        SslSettings.builder().applySettings(customSettings).build() == customSettings
        SslSettings.builder(customSettings).applySettings(defaultSettings).build() == defaultSettings
    }

    @IgnoreIf({ isNotAtLeastJava7() })
    def 'should apply builder settings'() {
        when:
        def original = SslSettings.builder().enabled(true)
                .context(SSLContext.getDefault())
                .invalidHostNameAllowed(true).build()

        def settings = SslSettings.builder(original).build()

        then:
        original == settings
    }

    def 'equivalent settings should be equal and have the same hash code'() {
        expect:
        SslSettings.builder().build() == SslSettings.builder().build()
        SslSettings.builder().build().hashCode() == SslSettings.builder().build().hashCode()
        SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build() ==
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build()
        SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build().hashCode() ==
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true).build().hashCode()
        SslSettings.builder().enabled(true).invalidHostNameAllowed(true).context(SSLContext.getDefault()).build() ==
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true)
                        .context(SSLContext.getDefault()).build()
        SslSettings.builder().enabled(true).invalidHostNameAllowed(true)
                .context(SSLContext.getDefault()).build().hashCode() ==
                SslSettings.builder().enabled(true).invalidHostNameAllowed(true)
                        .context(SSLContext.getDefault()).build().hashCode()
    }

    @IgnoreIf({ isNotAtLeastJava7() })
    def 'unequivalent settings should not be equal or have the same hash code'() {
        expect:
        SslSettings.builder().build() != SslSettings.builder().enabled(true).build()
        SslSettings.builder().build() != SslSettings.builder().invalidHostNameAllowed(true).build()
        SslSettings.builder().build() != SslSettings.builder().context(SSLContext.getDefault()).build()
    }
}
