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

import com.mongodb.ConnectionString
import com.mongodb.MongoInternalException
import spock.lang.IgnoreIf
import spock.lang.Specification

import static com.mongodb.connection.SslSettings.builder


class SslSettingsSpecification extends Specification {
    def 'should default to disabled'() {
        expect:
        !builder().build().enabled
    }

    @IgnoreIf({ System.getProperty('java.version').startsWith('1.6.') })
    def 'should enable'() {
        expect:
        builder().enabled(true).build().enabled
    }

    def 'should default to disallow invalid host name'() {
        expect:
        !builder().build().invalidHostNameAllowed
    }

    def 'should allow invalid host name'() {
        expect:
        builder().invalidHostNameAllowed(true).build().invalidHostNameAllowed
    }

    def 'should not allow invalid host name on Java 6'() {
        given:
        String javaVersion = System.getProperty('java.version')
        when:

        System.setProperty('java.version', '1.6.0_45')
        builder().enabled(true).build()

        then:
        def e = thrown(MongoInternalException)
        e.message.contains('SslSettings.invalidHostNameAllowed')

        cleanup:
        System.setProperty('java.version', javaVersion)
    }

    def 'should apply connection string without ssl'() {
        expect:
        !builder().applyConnectionString(new ConnectionString('mongodb://localhost')).build().enabled
        !builder().applyConnectionString(new ConnectionString('mongodb://localhost/?ssl=false')).build().enabled
        !builder().applyConnectionString(new ConnectionString('mongodb://localhost')).build().invalidHostNameAllowed
        !builder().applyConnectionString(new ConnectionString('mongodb://localhost/?ssl=false')).build().invalidHostNameAllowed
    }

    @IgnoreIf({ System.getProperty('java.version').startsWith('1.6.') })
    def 'should apply connection string with ssl'() {
        expect:
        builder().applyConnectionString(new ConnectionString('mongodb://localhost/?ssl=true')).build().enabled
        !builder().applyConnectionString(new ConnectionString('mongodb://localhost/?ssl=true')).build().invalidHostNameAllowed
    }

    @IgnoreIf({ System.getProperty('java.version').startsWith('1.6.') })
    def 'should apply connection string with ssl and sslInvalidHostNameAllowed'() {
        expect:
        builder().applyConnectionString(new ConnectionString('mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true'))
                .build().enabled
        builder().applyConnectionString(new ConnectionString('mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true'))
                .build().invalidHostNameAllowed
    }

    def 'should apply connection string with ssl and invalidHostNameAllowed'() {
        expect:
        builder().applyConnectionString(new ConnectionString('mongodb://localhost/?ssl=true')).invalidHostNameAllowed(true).build().enabled
    }

    def 'equivalent settings should be equal and have the same hash code'() {
        expect:
        builder().build() == builder().build()
        builder().build().hashCode() == builder().build().hashCode()
        builder().enabled(true).invalidHostNameAllowed(true).build() == builder().enabled(true).invalidHostNameAllowed(true).build()
        builder().enabled(true).invalidHostNameAllowed(true).build().hashCode() ==
        builder().enabled(true).invalidHostNameAllowed(true).build().hashCode()
    }

    @IgnoreIf({ System.getProperty('java.version').startsWith('1.6.') })
    def 'unequivalent settings should not be equal or have the same hash code'() {
        expect:
        builder().build() != builder().enabled(true).build()
        builder().build() != builder().invalidHostNameAllowed(true).build()
    }
}
