/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model

import com.mongodb.OperationFunctionalSpecification
import org.bson.Document
import org.bson.conversions.Bson
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Updates.bitwiseAnd
import static com.mongodb.client.model.Updates.bitwiseOr
import static com.mongodb.client.model.Updates.bitwiseXor

class BitwiseUpdatesFunctionalSpecification extends OperationFunctionalSpecification {
    private static final long LONG_MASK = 0x0fffffffffffffff
    private static final int INT_MASK = 0x0ffffffff;
    private static final int NUM = 13

    def a = new Document('_id', 1).append('x', NUM)


    def setup() {
        getCollectionHelper().insertDocuments(a)
    }

    def find() {
        getCollectionHelper().find(new Document('_id', 1))
    }


    def updateOne(Bson update) {
        getCollectionHelper().updateOne(new Document('_id', 1), update)
    }

    def 'integer bitwiseAnd'() {
        when:
        updateOne(bitwiseAnd('x', INT_MASK))

        then:
        find() == [new Document('_id', 1).append('x', NUM & INT_MASK)]
    }

    def 'integer bitwiseOr'() {
        when:
        updateOne(bitwiseOr('x', INT_MASK))

        then:
        find() == [new Document('_id', 1).append('x', NUM | INT_MASK)]
    }

    @IgnoreIf({ !serverVersionAtLeast(2, 6) })
    def 'integer bitwiseXor'() {
        when:
        updateOne(bitwiseXor('x', INT_MASK))

        then:
        find() == [new Document('_id', 1).append('x', NUM ^ INT_MASK)]
    }

    def 'long bitwiseAnd'() {
        when:
        updateOne(bitwiseAnd('x', LONG_MASK))

        then:
        find() == [new Document('_id', 1).append('x', NUM & LONG_MASK)]
    }

    @IgnoreIf({ !serverVersionAtLeast(2, 6) })     // a bug in the 2.4 server prevents this test from passing
    def 'long bitwiseOr'() {
        when:
        updateOne(bitwiseOr('x', LONG_MASK))

        then:
        find() == [new Document('_id', 1).append('x', NUM | LONG_MASK)]
    }

    @IgnoreIf({ !serverVersionAtLeast(2, 6) })
    def 'long bitwiseXor'() {
        when:
        updateOne(bitwiseXor('x', LONG_MASK))

        then:
        find() == [new Document('_id', 1).append('x', NUM ^ LONG_MASK)]
    }

}
