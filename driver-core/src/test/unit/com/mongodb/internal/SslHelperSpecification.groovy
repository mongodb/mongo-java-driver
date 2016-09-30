/*
 * Copyright 2015-2016 MongoDB, Inc.
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

package com.mongodb.internal

import com.mongodb.ServerAddress
import com.mongodb.internal.connection.SslHelper
import spock.lang.IgnoreIf
import spock.lang.Specification

import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLParameters

import static com.mongodb.ClusterFixture.isNotAtLeastJava7
import static com.mongodb.ClusterFixture.isNotAtLeastJava8

@IgnoreIf({ isNotAtLeastJava7() })
class SslHelperSpecification extends Specification {
    def 'should enable HTTPS host name verification'() {
        given:
        def sslParameters = new SSLParameters()

        when:
        SslHelper.enableHostNameVerification(sslParameters)

        then:
        sslParameters.getEndpointIdentificationAlgorithm() == 'HTTPS'
    }

    @IgnoreIf({ isNotAtLeastJava8() })
    def 'should enable server name indicator'() {
        given:
        def serverName = 'server.me'
        def sslParameters = new SSLParameters()

        when:
        SslHelper.enableSni(new ServerAddress(serverName), sslParameters)

        then:
        sslParameters.getServerNames() == [new SNIHostName(serverName)]
    }
}