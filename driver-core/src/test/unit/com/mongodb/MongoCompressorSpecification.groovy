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

class MongoCompressorSpecification extends Specification {
    def 'should create zlib compressor'() {
        when:
        def compressor = MongoCompressor.createZlibCompressor()

        then:
        compressor.getName() == 'zlib'
        compressor.getProperty(MongoCompressor.LEVEL, -1) == -1
    }

    def 'should set property'() {
        when:
        def compressor = MongoCompressor.createZlibCompressor()
        def newCompressor = compressor.withProperty(MongoCompressor.LEVEL, 5)

        then:
        compressor != newCompressor
        compressor.getProperty(MongoCompressor.LEVEL, -1) == -1
        compressor.getProperty(MongoCompressor.LEVEL, 5) == 5
    }


}
