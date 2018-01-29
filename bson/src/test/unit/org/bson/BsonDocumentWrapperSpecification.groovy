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

package org.bson

import org.bson.codecs.DocumentCodec
import spock.lang.Specification

import static java.util.Arrays.asList

class BsonDocumentWrapperSpecification extends Specification {
    def document = new Document()
            .append('a', 1)
            .append('b', 2)
            .append('c', asList('x', true))
            .append('d', asList(new Document('y', false), 1));

    def wrapper = new BsonDocumentWrapper(document, new DocumentCodec())

    def 'should serialize and deserialize'() {
        given:
        def baos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(baos)

        when:
        oos.writeObject(wrapper)
        def bais = new ByteArrayInputStream(baos.toByteArray())
        def ois = new ObjectInputStream(bais)
        def deserializedDocument = ois.readObject()

        then:
        wrapper == deserializedDocument
    }
}
