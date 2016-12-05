/*
 * Copyright 2015 MongoDB, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import com.mongodb.client.model.geojson.MultiPolygon
import org.bson.BsonDocument
import org.bson.Document
import spock.lang.Specification

class MongoClientSpecification extends Specification {

    def 'default codec registry should contain all supported providers'() {
        given:
        def codecRegistry = MongoClient.getDefaultCodecRegistry()

        expect:
        codecRegistry.get(BsonDocument)
        codecRegistry.get(BasicDBObject)
        codecRegistry.get(Document)
        codecRegistry.get(Integer)
        codecRegistry.get(MultiPolygon)
        codecRegistry.get(Iterable)
    }
}