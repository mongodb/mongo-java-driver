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





package org.mongodb

import spock.lang.Specification

import static com.mongodb.operation.AggregationOptions.OutputMode.CURSOR
import static com.mongodb.operation.AggregationOptions.OutputMode.INLINE
import static com.mongodb.operation.AggregationOptions.builder
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class AggregationOptionsSpecification extends Specification {

    def "equal options should be equal"() {
        expect:
        builder().build() == builder().build()
        builder().allowDiskUse(true).batchSize(3).outputMode(CURSOR).maxTime(42, MILLISECONDS).build() ==
        builder().allowDiskUse(true).batchSize(3).outputMode(CURSOR).maxTime(42, MILLISECONDS).build()
    }

    def "unequal options should not be equal"() {
        expect:
        builder().allowDiskUse(true).build() != builder().allowDiskUse(false).build()
        builder().batchSize(3).build() != builder().build()
        builder().outputMode(CURSOR).build() != builder().outputMode(INLINE).build()
        builder().maxTime(42, MILLISECONDS).build() != builder().maxTime(42, SECONDS).build()
    }

    def "AggregateOptions.Builder should build with all values"() {
        builder().allowDiskUse(true).build().allowDiskUse
        builder().batchSize(3).build().batchSize == 3
        builder().outputMode(CURSOR).build().outputMode == CURSOR
        builder().maxTime(42, MILLISECONDS).build().getMaxTime(MILLISECONDS) == 42
    }
}