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

package com.mongodb.operation

import category.Async
import org.bson.BsonDocument
import org.bson.BsonString
import org.junit.experimental.categories.Category
import org.mongodb.AggregationOptions
import org.mongodb.Document
import org.mongodb.FunctionalSpecification

import static java.util.Arrays.asList
import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getBinding
import static org.mongodb.Fixture.serverVersionAtLeast

class AggregateExplainOperationSpecification extends FunctionalSpecification {

    def 'should be able to explain an empty pipeline'() {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0)))

        given:
        AggregateExplainOperation op = new AggregateExplainOperation(getNamespace(), [], aggregateOptions)

        when:
        def result = op.execute(getBinding());

        then:
        result.getResponse().containsKey('stages')

        where:
        aggregateOptions << [
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build(),
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build()]
    }

    @Category(Async)
    def 'should be able to explain an empty pipeline asynchronously'() {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0)))

        given:
        AggregateExplainOperation op = new AggregateExplainOperation(getNamespace(), [], aggregateOptions)

        when:
        def result = op.executeAsync(getAsyncBinding()).get();

        then:
        result.getResponse().containsKey('stages')

        where:
        aggregateOptions << [
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build(),
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build()]
    }

    def 'should be able to explain a pipeline'() {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0)))

        given:
        def match = new BsonDocument('job', new BsonString('plumber'))
        AggregateExplainOperation op = new AggregateExplainOperation(getNamespace(),
                                                                     [new BsonDocument('$match', match)],
                                                                     aggregateOptions)

        when:
        def result = op.execute(getBinding());

        then:
        result.getResponse().containsKey('stages')
        Document stage = (Document) result.getResponse().get('stages').first()
        stage.'$cursor'.'query' == match

        where:
        aggregateOptions << [
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build(),
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build()]
    }

    @Category(Async)
    def 'should be able to explain a pipeline asynchronously'() {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0)))

        given:
        def match = new BsonDocument('job', new BsonString('plumber'))
        AggregateExplainOperation op = new AggregateExplainOperation(getNamespace(),
                                                                     [new BsonDocument('$match', match)],
                                                                     aggregateOptions)
        when:
        def result = op.executeAsync(getAsyncBinding()).get();

        then:
        result.getResponse().containsKey('stages')
        Document stage = (Document) result.getResponse().get('stages').first()
        stage.'$cursor'.'query' == match

        where:
        aggregateOptions << [
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.INLINE).build(),
                AggregationOptions.builder().outputMode(AggregationOptions.OutputMode.CURSOR).build()]
    }
}
