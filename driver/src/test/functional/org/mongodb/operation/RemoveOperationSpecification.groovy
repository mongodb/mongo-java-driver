/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation

import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.WriteConcern
import org.mongodb.codecs.DocumentCodec

import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getSession

class RemoveOperationSpecification extends FunctionalSpecification {
    def 'should remove a document'() {
        given:
        def insert = new Insert<Document>(WriteConcern.ACKNOWLEDGED, new Document('_id', 1))
        new InsertOperation<Document>(getNamespace(), insert, new DocumentCodec(), getBufferProvider(), getSession(),
                true).execute()
        def op = new RemoveOperation(getNamespace(), WriteConcern.ACKNOWLEDGED,
                [new Remove(WriteConcern.ACKNOWLEDGED, new Document('_id', 1))], new DocumentCodec(), getBufferProvider(), getSession(),
                true)

        when:
        op.execute()

        then:
        collection.find().count() == 0
    }

    def 'should split removes into batches'() {
        given:
        def insert = new Insert<Document>(WriteConcern.ACKNOWLEDGED,
                [new Document('_id', 1)])
        new InsertOperation<Document>(getNamespace(), insert, new DocumentCodec(), getBufferProvider(), getSession(),
                true).execute()
        def op = new RemoveOperation(getNamespace(), WriteConcern.ACKNOWLEDGED,
                [new Remove(WriteConcern.ACKNOWLEDGED, new Document('_id', 1))], new DocumentCodec(), getBufferProvider(), getSession(),
                true)

        when:
        op.execute()

        then:
        collection.find().count() == 0

    }
}