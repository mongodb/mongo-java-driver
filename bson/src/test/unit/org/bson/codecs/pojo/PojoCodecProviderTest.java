/*
 * Copyright 2017 MongoDB, Inc.
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

package org.bson.codecs.pojo;

import org.bson.codecs.Codec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.entities.NestedGenericHolderModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.junit.Test;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.pojo.Conventions.NO_CONVENTIONS;
import static org.junit.Assert.assertNull;

public final class PojoCodecProviderTest extends PojoTestCase {

    @Test
    public void testClassNotFound() {
        PojoCodecProvider provider = PojoCodecProvider.builder().build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<SimpleModel> codec = provider.get(SimpleModel.class, registry);
        assertNull(codec);
    }

    @Test
    public void testPackageLessClasses() {
        PojoCodecProvider provider = PojoCodecProvider.builder().build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<Byte> codec = provider.get(byte.class, registry);
        assertNull(codec);
    }

    @Test
    public void testRegisterClassModel() {
        ClassModel<SimpleModel> classModel = ClassModel.builder(SimpleModel.class).build();
        PojoCodecProvider provider = PojoCodecProvider.builder().register(classModel).build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());

        SimpleModel model = getSimpleModel();
        roundTrip(registry, model, "{'integerField': 42, 'stringField': 'myString'}");
    }

    @Test
    public void testRegisterClass() {
        PojoCodecProvider provider = PojoCodecProvider.builder().register(SimpleModel.class).build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());

        SimpleModel model = getSimpleModel();
        roundTrip(registry, model, "{'integerField': 42, 'stringField': 'myString'}");
    }

    @Test
    public void testRegisterPackage() {
        PojoCodecProvider provider = PojoCodecProvider.builder().register("org.bson.codecs.pojo.entities").build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());

        roundTrip(registry, getSimpleModel(), "{'integerField': 42, 'stringField': 'myString'}");
    }

    @Test
    public void testRegisterClassModelPreferredOverClass() {
        ClassModel<SimpleModel> classModel = ClassModel.builder(SimpleModel.class).enableDiscriminator(false).build();
        PojoCodecProvider provider = PojoCodecProvider.builder().register(SimpleModel.class).register(classModel).build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());

        SimpleModel model = getSimpleModel();
        roundTrip(registry, model, "{'integerField': 42, 'stringField': 'myString'}");
    }

    @Test
    public void testConventions() {
        PojoCodecProvider provider = PojoCodecProvider.builder().conventions(NO_CONVENTIONS)
                .register("org.bson.codecs.pojo.entities").build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());

        roundTrip(registry, getConventionModel(),
                "{'myFinalField': 10, 'myIntField': 10, 'customId': 'id',"
                        + "'child': {'myFinalField': 10, 'myIntField': 10, 'customId': 'child',"
                        + "          'simpleModel': {'integerField': 42, 'stringField': 'myString' } } }");
    }

    @Test(expected = CodecConfigurationException.class)
    public void testThrowsIfThereAreMissingCodecs(){
        ClassModel<NestedGenericHolderModel> classModel =
                ClassModel.builder(NestedGenericHolderModel.class).build();
        PojoCodecProvider provider = PojoCodecProvider.builder().register(classModel).build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        registry.get(NestedGenericHolderModel.class);
    }

}
