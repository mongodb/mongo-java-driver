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

class BucketGranularitySpecification extends Specification {
    def 'should return the expected string value'() {
        expect:
        granularity.getValue() == expectedString

        where:
        granularity                  | expectedString
        BucketGranularity.R5         | 'R5'
        BucketGranularity.R10        | 'R10'
        BucketGranularity.R20        | 'R20'
        BucketGranularity.R40        | 'R40'
        BucketGranularity.R80        | 'R80'
        BucketGranularity.SERIES_125 | '1-2-5'
        BucketGranularity.E6         | 'E6'
        BucketGranularity.E12        | 'E12'
        BucketGranularity.E24        | 'E24'
        BucketGranularity.E48        | 'E48'
        BucketGranularity.E96        | 'E96'
        BucketGranularity.E192       | 'E192'
        BucketGranularity.POWERSOF2  | 'POWERSOF2'
    }
}
