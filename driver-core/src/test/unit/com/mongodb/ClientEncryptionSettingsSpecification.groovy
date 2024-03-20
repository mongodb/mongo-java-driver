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

import javax.net.ssl.SSLContext
import java.util.concurrent.TimeUnit

class ClientEncryptionSettingsSpecification extends Specification {

    def 'should have return the configured values defaults'() {
        given:
        def mongoClientSettings = MongoClientSettings.builder().build()
        def keyVaultNamespace = "keyVaultNamespace"
        def kmsProvider = ["provider": ["test" : "test"]]
        def kmsProviderSupplier = ["provider": () -> ["test" : "test"]]
        def kmsProviderSslContextMap = ["provider": SSLContext.getDefault()]

        when:
        def options = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(mongoClientSettings)
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProvider)
                .build()

        then:
        options.getKeyVaultMongoClientSettings() == mongoClientSettings
        options.getKeyVaultNamespace() == keyVaultNamespace
        options.getKmsProviders() == kmsProvider
        options.getKmsProviderPropertySuppliers() == [:]
        options.getKmsProviderSslContextMap() == [:]
        options.getTimeout(TimeUnit.MILLISECONDS) == null

        when:
        options = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(mongoClientSettings)
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProvider)
                .kmsProviderPropertySuppliers(kmsProviderSupplier)
                .kmsProviderSslContextMap(kmsProviderSslContextMap)
                .timeout(1_000, TimeUnit.MILLISECONDS)
                .build()

        then:
        options.getKeyVaultMongoClientSettings() == mongoClientSettings
        options.getKeyVaultNamespace() == keyVaultNamespace
        options.getKmsProviders() == kmsProvider
        options.getKmsProviderPropertySuppliers() == kmsProviderSupplier
        options.getKmsProviderSslContextMap() == kmsProviderSslContextMap
        options.getTimeout(TimeUnit.MILLISECONDS) == 1_000
    }

    def 'should throw an exception if the defaultTimeout is set and negative'() {
        given:
        def builder = ClientEncryptionSettings.builder()

        when:
        builder.timeout(500, TimeUnit.NANOSECONDS)

        then:
        thrown(IllegalArgumentException)

        when:
        builder.timeout(-1, TimeUnit.SECONDS)

        then:
        thrown(IllegalArgumentException)
    }

}
