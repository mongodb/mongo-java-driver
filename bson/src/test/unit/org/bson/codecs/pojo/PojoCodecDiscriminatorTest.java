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

package org.bson.codecs.pojo;

import org.bson.codecs.pojo.entities.DiscriminatorModel;
import org.bson.codecs.pojo.entities.DiscriminatorWithGetterModel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public final class PojoCodecDiscriminatorTest extends PojoTestCase {

    @Test
    public void testDiscriminatorEncodedOnceWhenItIsAlsoAGetter() {
        byte[] encodedDiscriminatorWithoutGetter = encode(
                getCodec(DiscriminatorModel.class),
                new DiscriminatorModel(),
                false
        ).toByteArray();
        byte[] encodedDiscriminatorWithGetter = encode(
                getCodec(DiscriminatorWithGetterModel.class),
                new DiscriminatorWithGetterModel(),
                false
        ).toByteArray();
        assertArrayEquals(encodedDiscriminatorWithoutGetter, encodedDiscriminatorWithGetter);
    }

    @Test
    public void testDiscriminatorEncodingWhenItIsAlsoAGetter() {
        encodesTo(
                getCodec(DiscriminatorWithGetterModel.class),
                new DiscriminatorWithGetterModel(),
                "{discriminatorKey:'discriminatorValue'}"
        );
    }
}
