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
package com.mongodb.client.model.search;

import com.mongodb.internal.client.model.AbstractConstructibleBsonElement;

import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

final class PhraseConstructibleBsonElement extends AbstractConstructibleBsonElement<PhraseConstructibleBsonElement> implements
        PhraseSearchOperator {
    PhraseConstructibleBsonElement(final String name, final Bson value) {
        super(name, value);
    }

    private PhraseConstructibleBsonElement(final Bson baseElement, final Bson appendedElementValue) {
        super(baseElement, appendedElementValue);
    }

    @Override
    protected PhraseConstructibleBsonElement newSelf(final Bson baseElement, final Bson appendedElementValue) {
        return new PhraseConstructibleBsonElement(baseElement, appendedElementValue);
    }

    @Override
    public PhraseSearchOperator synonyms(final String name) {
        return newWithAppendedValue("synonyms", notNull("name", name));
    }

    @Override
    public PhraseSearchOperator slop(final int slop) {
        return newWithAppendedValue("slop", notNull("slop", slop));
    }

    @Override
    public PhraseConstructibleBsonElement score(final SearchScore modifier) {
        return newWithAppendedValue("score", notNull("modifier", modifier));
    }
}
