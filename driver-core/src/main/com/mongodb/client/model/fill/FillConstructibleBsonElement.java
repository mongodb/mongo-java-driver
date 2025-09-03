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
package com.mongodb.client.model.fill;

import com.mongodb.internal.client.model.AbstractConstructibleBsonElement;
import org.bson.conversions.Bson;

final class FillConstructibleBsonElement extends AbstractConstructibleBsonElement<FillConstructibleBsonElement> implements
        ValueFillOutputField, LocfFillOutputField, LinearFillOutputField {
    FillConstructibleBsonElement(final String name, final Bson value) {
        super(name, value);
    }

    FillConstructibleBsonElement(final Bson baseElement) {
        super(baseElement);
    }

    private FillConstructibleBsonElement(final Bson baseElement, final Bson appendedElementValue) {
        super(baseElement, appendedElementValue);
    }

    @Override
    protected FillConstructibleBsonElement newSelf(final Bson baseElement, final Bson appendedElementValue) {
        return new FillConstructibleBsonElement(baseElement, appendedElementValue);
    }
}
