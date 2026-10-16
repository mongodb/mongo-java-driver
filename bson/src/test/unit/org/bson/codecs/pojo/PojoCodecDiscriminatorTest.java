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

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.pojo.entities.DiscriminatorModel;
import org.bson.codecs.pojo.entities.DiscriminatorWithGetterModel;
import org.bson.codecs.pojo.entities.DiscriminatorWithProperty;
import org.bson.codecs.pojo.entities.DiscriminatorWithPropertyAndIgnore;
import org.bson.codecs.pojo.entities.ShapeHolderModel;
import org.bson.codecs.pojo.entities.ShapeModelAbstract;
import org.bson.codecs.pojo.entities.ShapeModelCircle;
import org.bson.codecs.pojo.entities.StaticInitializerTrackingModel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class PojoCodecDiscriminatorTest extends PojoTestCase {

    @Test
    public void testDiscriminatorEncodedOnceWhenItIsAlsoAGetter() {
        byte[] encodedDiscriminatorModel = encode(
                getCodec(DiscriminatorModel.class),
                new DiscriminatorModel(),
                false
        ).toByteArray();
        byte[] encodedDiscriminatorWithGetter = encode(
                getCodec(DiscriminatorWithGetterModel.class),
                new DiscriminatorWithGetterModel(),
                false
        ).toByteArray();
        assertArrayEquals(encodedDiscriminatorModel, encodedDiscriminatorWithGetter);
    }

    @Test
    public void testDiscriminatorRoundTripWhenItIsAlsoAGetter() {
        roundTrip(
                new DiscriminatorWithGetterModel(),
                "{discriminatorKey:'discriminatorValue'}"
        );
    }

    @Test
    public void testDiscriminatorEncodedOnceWhenItIsAlsoAProperty() {
        byte[] encodedDiscriminatorModel = encode(
                getCodec(DiscriminatorModel.class),
                new DiscriminatorModel(),
                false
        ).toByteArray();
        byte[] encodedDiscriminatorWithProperty = encode(
                getCodec(DiscriminatorWithProperty.class),
                new DiscriminatorWithProperty(),
                false
        ).toByteArray();
        assertArrayEquals(encodedDiscriminatorModel, encodedDiscriminatorWithProperty);
    }

    @Test
    public void testDiscriminatorRoundTripWhenItIsAlsoAProperty() {
        roundTrip(
                new DiscriminatorWithProperty(),
                "{discriminatorKey:'discriminatorValue'}"
        );
    }

    @Test
    public void testDiscriminatorRoundTripWhenItIsAlsoAPropertyWithIgnoredProperty() {
        roundTrip(
                new DiscriminatorWithPropertyAndIgnore(),
                "{discriminatorKey:'discriminatorValue'}"
        );
    }

    @Test
    public void testDiscriminatorForUnrelatedClassIsRejectedWithoutInitializing() {
        Codec<ShapeModelCircle> codec = getCodec(ShapeModelCircle.class);
        String json = "{_t: '" + StaticInitializerTrackingModel.class.getName() + "', color: 'red'}";

        assertThrows(CodecConfigurationException.class, () -> decode(codec, json));
        assertFalse(StaticInitializerTrackingModel.InitializationTracker.initialized);
    }

    @Test
    public void testDiscriminatorForUnrelatedClassIsRejectedForNestedProperty() {
        Codec<ShapeHolderModel> codec = getCodec(getPojoCodecProviderBuilder(ShapeHolderModel.class, ShapeModelAbstract.class,
                ShapeModelCircle.class), ShapeHolderModel.class);
        String json = "{shape: {_t: '" + StaticInitializerTrackingModel.class.getName() + "', color: 'red'}}";

        assertThrows(CodecConfigurationException.class, () -> decode(codec, json));
        assertFalse(StaticInitializerTrackingModel.InitializationTracker.initialized);
    }

    @Test
    public void testDiscriminatorOfRegisteredUnrelatedClassIsRejected() {
        Codec<ShapeModelCircle> codec = getCodec(getPojoCodecProviderBuilder(ShapeModelCircle.class, DiscriminatorModel.class),
                ShapeModelCircle.class);

        assertThrows(CodecConfigurationException.class, () -> decode(codec, "{_t: 'discriminatorValue'}"));
    }

    @Test
    public void testDiscriminatorForSubclassIsAccepted() {
        Codec<ShapeModelAbstract> codec = getCodec(getPojoCodecProviderBuilder(ShapeModelAbstract.class, ShapeModelCircle.class),
                ShapeModelAbstract.class);

        decodesTo(codec, "{_t: '" + ShapeModelCircle.class.getName() + "', color: 'red', radius: 1.0}", new ShapeModelCircle("red", 1.0));
    }

    private static <T> T decode(final Codec<T> codec, final String json) {
        return codec.decode(new BsonDocumentReader(BsonDocument.parse(json)), DecoderContext.builder().build());
    }
}
