/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb.client.model

import spock.lang.Specification

import static com.mongodb.client.model.Accumulators.sum

class BucketAutoOptionsSpecification extends Specification {
    def "should return new options with the same property values"() {
        when:
        def options = new BucketAutoOptions()
            .granularity(BucketGranularity.E96)
                .output(sum('count', 1))

        then:
        def sum = sum('count', 1)
        options.granularity == BucketGranularity.E96
        options.output.size() == 1
        options.output[0].name == sum.name
    }

}
