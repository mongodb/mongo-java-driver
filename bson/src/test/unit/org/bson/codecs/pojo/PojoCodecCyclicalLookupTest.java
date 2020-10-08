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

import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.entities.ConventionModel;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.GenericTreeModel;
import org.bson.codecs.pojo.entities.ListListGenericExtendedModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderFieldWithMultipleTypeParamsModel;
import org.bson.codecs.pojo.entities.NestedGenericTreeModel;
import org.bson.codecs.pojo.entities.PropertyWithMultipleTypeParamsModel;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class PojoCodecCyclicalLookupTest extends PojoTestCase {

    @Test
    void testSimpleModel() {
        SimpleModel model = getSimpleModel();
        LookupCountingCodecRegistry registry = createRegistry(SimpleModel.class);
        roundTrip(registry, model, SIMPLE_MODEL_JSON);

        assertEquals(2, registry.counters.get(SimpleModel.class).get()); // Looked up in encodesTo & decodesTo
        assertEquals(1, registry.counters.get(String.class).get()); // Lookup on encode then cached (PropertyCodecRegistry)
        assertEquals(1, registry.counters.get(Integer.class).get()); // Lookup on encode then cached (PropertyCodecRegistry)
    }

    @Test
    void testConventionModel() {
        ConventionModel model = getConventionModel();
        String json = "{'_id': 'id', '_cls': 'AnnotatedConventionModel', 'myFinalField': 10, 'myIntField': 10,"
                + "'child': {'_id': 'child', 'myFinalField': 10, 'myIntField': 10,"
                + "'model': {'integerField': 42, 'stringField': 'myString'}}}";
        LookupCountingCodecRegistry registry = createRegistry(ConventionModel.class, SimpleModel.class);
        roundTrip(registry, model, json);

        assertEquals(2, registry.counters.get(ConventionModel.class).get()); // Looked up in encodesTo & decodesTo
        assertEquals(1, registry.counters.get(SimpleModel.class).get()); // Lookup on encode then cached (PropertyCodecRegistry)
        assertEquals(2, registry.counters.get(String.class).get()); // Once for ConventionModel & once for SimpleModel
        assertEquals(2, registry.counters.get(Integer.class).get()); // Once for ConventionModel & once for SimpleModel
    }

    @Test
    void testNestedGenericTreeModel() {
        NestedGenericTreeModel model = new NestedGenericTreeModel(42, getGenericTreeModel());
        String json = "{'intField': 42, 'nested': {'field1': 'top', 'field2': 1, "
                + "'left': {'field1': 'left', 'field2': 2, 'left': {'field1': 'left', 'field2': 3}}, "
                + "'right': {'field1': 'right', 'field2': 4, 'left': {'field1': 'left', 'field2': 5}}}}";
        LookupCountingCodecRegistry registry = createRegistry(NestedGenericTreeModel.class, GenericTreeModel.class);
        roundTrip(registry, model, json);

        assertEquals(2, registry.counters.get(NestedGenericTreeModel.class).get());
        assertEquals(1, registry.counters.get(GenericTreeModel.class).get());
        assertEquals(1, registry.counters.get(String.class).get());
        assertEquals(1, registry.counters.get(Integer.class).get());
    }

    @Test
    void testNestedGenericHolderFieldWithMultipleTypeParamsModel() {
        NestedGenericHolderFieldWithMultipleTypeParamsModel model = getNestedGenericHolderFieldWithMultipleTypeParamsModel();
        LookupCountingCodecRegistry registry = createRegistry(NestedGenericHolderFieldWithMultipleTypeParamsModel.class,
                        PropertyWithMultipleTypeParamsModel.class, SimpleGenericsModel.class, GenericHolderModel.class);
        String json = "{'nested': {'myGenericField': {_t: 'PropertyWithMultipleTypeParamsModel', "
                        + "'simpleGenericsModel': {_t: 'org.bson.codecs.pojo.entities.SimpleGenericsModel', 'myIntegerField': 42, "
                        + "'myGenericField': {'$numberLong': '101'}, 'myListField': ['B', 'C'], 'myMapField': {'D': 2, 'E': 3, 'F': 4 }}},"
                        + "'myLongField': {'$numberLong': '42'}}}";


        roundTrip(registry, model, json);

        assertEquals(2, registry.counters.get(NestedGenericHolderFieldWithMultipleTypeParamsModel.class).get());
        assertEquals(1, registry.counters.get(PropertyWithMultipleTypeParamsModel.class).get());
        assertEquals(1, registry.counters.get(SimpleGenericsModel.class).get());
        assertEquals(1, registry.counters.get(GenericHolderModel.class).get());
        assertEquals(1, registry.counters.get(Long.class).get());
        assertEquals(1, registry.counters.get(String.class).get());
        assertEquals(1, registry.counters.get(Integer.class).get());
    }

    @Test
    void testListListGenericExtendedModel() {
        ListListGenericExtendedModel model = new ListListGenericExtendedModel(asList(asList(1, 2, 3), asList(4, 5, 6)));
        LookupCountingCodecRegistry registry = createRegistry(ListListGenericExtendedModel .class);
        String json = "{values: [[1, 2, 3], [4, 5, 6]]}";
        roundTrip(registry, model, json);

        assertEquals(2, registry.counters.get(ListListGenericExtendedModel.class).get());
        assertEquals(1, registry.counters.get(Integer.class).get());
    }


    LookupCountingCodecRegistry createRegistry(final Class<?>... classes) {
        return new LookupCountingCodecRegistry(
                new BsonValueCodecProvider(),
                new ValueCodecProvider(),
                getPojoCodecProviderBuilder(classes).build()
        );
    }


    static class LookupCountingCodecRegistry implements CodecRegistry {

        private final ConcurrentHashMap<Class<?>, AtomicInteger> counters;
        private final List<CodecProvider> codecProviders;

        LookupCountingCodecRegistry(final CodecProvider... providers) {
            this.codecProviders = asList(providers);
            this.counters = new ConcurrentHashMap<>();
        }

        @Override
        public <T> Codec<T> get(final Class<T> clazz) {
            incrementCount(clazz);
            for (CodecProvider provider : codecProviders) {
                Codec<T> codec = provider.get(clazz, this);
                if (codec != null) {
                    return codec;
                }
            }
            return null;
        }

        public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
            incrementCount(clazz);
            for (CodecProvider provider : codecProviders) {
                Codec<T> codec = provider.get(clazz, registry);
                if (codec != null) {
                    return codec;
                }
            }
            return null;
        }

        private synchronized <T> void incrementCount(final Class<T> clazz) {
            AtomicInteger atomicInteger = counters.computeIfAbsent(clazz, k -> new AtomicInteger());
            atomicInteger.incrementAndGet();
        }
    }

}
