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

package com.mongodb.binding

import com.mongodb.ServerAddress
import com.mongodb.connection.Cluster
import spock.lang.Specification

class SingleServerBindingSpecification extends Specification {

    def 'should increment and decrement reference counts'() {
        given:
        def cluster = Mock(Cluster)
        def address = new ServerAddress()

        when:
        def binding = new SingleServerBinding(cluster, address)

        then:
        binding.count == 1

        when:
        def source = binding.getReadConnectionSource()

        then:
        source.count == 1
        binding.count == 2

        when:
        source.retain()

        then:
        source.count == 2
        binding.count == 2

        when:
        source.release()

        then:
        source.count == 1
        binding.count == 2

        when:
        source.release()

        then:
        source.count == 0
        binding.count == 1

        when:
        source = binding.getWriteConnectionSource()

        then:
        source.count == 1
        binding.count == 2

        when:
        source.retain()

        then:
        source.count == 2
        binding.count == 2

        when:
        source.release()

        then:
        source.count == 1
        binding.count == 2

        when:
        source.release()

        then:
        source.count == 0
        binding.count == 1

        when:
        binding.release()

        then:
        binding.count == 0
    }
}
