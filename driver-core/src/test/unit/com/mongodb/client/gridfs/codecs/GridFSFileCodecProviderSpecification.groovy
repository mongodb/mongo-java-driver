/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.gridfs.codecs

import com.mongodb.client.gridfs.model.GridFSFile
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecRegistries
import spock.lang.Specification

class GridFSFileCodecProviderSpecification extends Specification {
    private final provider = new GridFSFileCodecProvider()
    private final registry = CodecRegistries.fromProviders(provider, new DocumentCodecProvider(), new BsonValueCodecProvider(),
            new ValueCodecProvider())

    def 'should provide supported codec or null'() {
        expect:
        provider.get(GridFSFile, registry) instanceof GridFSFileCodec
        provider.get(TestType, registry) == null
    }

    @SuppressWarnings('EmptyClass')
    class TestType {
    }
}
