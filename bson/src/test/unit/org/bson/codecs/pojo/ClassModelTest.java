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

import org.bson.codecs.pojo.entities.CollectionNestedPojoModel;
import org.bson.codecs.pojo.entities.ConcreteAndNestedAbstractInterfaceModel;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.InterfaceBasedModel;
import org.bson.codecs.pojo.entities.ListGenericExtendedModel;
import org.bson.codecs.pojo.entities.ListListGenericExtendedModel;
import org.bson.codecs.pojo.entities.ListMapGenericExtendedModel;
import org.bson.codecs.pojo.entities.MapGenericExtendedModel;
import org.bson.codecs.pojo.entities.MapListGenericExtendedModel;
import org.bson.codecs.pojo.entities.MapMapGenericExtendedModel;
import org.bson.codecs.pojo.entities.MultipleBoundsModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderMapModel;
import org.bson.codecs.pojo.entities.PropertySelectionModel;
import org.bson.codecs.pojo.entities.ShapeHolderCircleModel;
import org.bson.codecs.pojo.entities.ShapeHolderModel;
import org.bson.codecs.pojo.entities.ShapeModelAbstract;
import org.bson.codecs.pojo.entities.ShapeModelCircle;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.SimpleWithStaticModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationInheritedModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationModel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public final class ClassModelTest {

    @Test
    public void testSimpleGenericsModel() {
        ClassModel<?> classModel = ClassModel.builder(SimpleGenericsModel.class).build();

        assertEquals("SimpleGenericsModel", classModel.getName());
        assertEquals(SimpleGenericsModel.class, classModel.getType());
        assertFalse(classModel.useDiscriminator());
        assertEquals("_t", classModel.getDiscriminatorKey());
        assertEquals("org.bson.codecs.pojo.entities.SimpleGenericsModel", classModel.getDiscriminator());
        assertNull(classModel.getIdPropertyModel());
        assertTrue(classModel.getInstanceCreatorFactory() instanceof InstanceCreatorFactoryImpl);

        assertEquals(4, classModel.getPropertyModels().size());
        assertEquals(classModel.getPropertyModel("myIntegerField").getTypeData(), createTypeData(Integer.class));
        assertEquals(classModel.getPropertyModel("myGenericField").getTypeData(), createTypeData(Object.class));
        assertEquals(classModel.getPropertyModel("myListField").getTypeData(), createTypeData(List.class, Object.class));
        assertEquals(classModel.getPropertyModel("myMapField").getTypeData(), createTypeData(Map.class, String.class, Object.class));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testCollectionNestedPojoModelPropertyTypes() {
        TypeData<String> string = createTypeData(String.class);
        TypeData<SimpleModel> simple = createTypeData(SimpleModel.class);
        TypeData<List> list = createBuilder(List.class).addTypeParameter(simple).build();
        TypeData<List> listList = createBuilder(List.class).addTypeParameter(list).build();
        TypeData<Set> set = createBuilder(Set.class).addTypeParameter(simple).build();
        TypeData<Set> setSet = createBuilder(Set.class).addTypeParameter(set).build();
        TypeData<Map> map = createBuilder(Map.class).addTypeParameter(string).addTypeParameter(simple).build();
        TypeData<List> listMap = createBuilder(List.class).addTypeParameter(map).build();
        TypeData<Map> mapMap = createBuilder(Map.class).addTypeParameter(string).addTypeParameter(map).build();
        TypeData<Map> mapList = createBuilder(Map.class).addTypeParameter(string).addTypeParameter(list).build();
        TypeData<Map> mapListMap = createBuilder(Map.class).addTypeParameter(string).addTypeParameter(listMap).build();
        TypeData<Map> mapSet = createBuilder(Map.class).addTypeParameter(string).addTypeParameter(set).build();
        TypeData<List> listMapList = createBuilder(List.class).addTypeParameter(mapList).build();
        TypeData<List> listMapSet = createBuilder(List.class).addTypeParameter(mapSet).build();

        ClassModel<?> classModel = ClassModel.builder(CollectionNestedPojoModel.class).build();

        assertEquals(12, classModel.getPropertyModels().size());
        assertEquals(classModel.getPropertyModel("listSimple").getTypeData(), list);
        assertEquals(classModel.getPropertyModel("listListSimple").getTypeData(), listList);

        assertEquals(classModel.getPropertyModel("setSimple").getTypeData(), set);
        assertEquals(classModel.getPropertyModel("setSetSimple").getTypeData(), setSet);

        assertEquals(classModel.getPropertyModel("mapSimple").getTypeData(), map);
        assertEquals(classModel.getPropertyModel("mapMapSimple").getTypeData(), mapMap);

        assertEquals(classModel.getPropertyModel("mapListSimple").getTypeData(), mapList);
        assertEquals(classModel.getPropertyModel("mapListMapSimple").getTypeData(), mapListMap);
        assertEquals(classModel.getPropertyModel("mapSetSimple").getTypeData(), mapSet);

        assertEquals(classModel.getPropertyModel("listMapSimple").getTypeData(), listMap);
        assertEquals(classModel.getPropertyModel("listMapListSimple").getTypeData(), listMapList);
        assertEquals(classModel.getPropertyModel("listMapSetSimple").getTypeData(), listMapSet);
    }

    @Test
    public void testWildcardModel() {
        ClassModel<?> classModel = ClassModel.builder(ConcreteAndNestedAbstractInterfaceModel.class).build();

        assertEquals(3, classModel.getPropertyModels().size());
        assertEquals(classModel.getPropertyModel("name").getTypeData(), createTypeData(String.class));
        assertEquals(classModel.getPropertyModel("child").getTypeData(), createTypeData(InterfaceBasedModel.class));
        assertEquals(classModel.getPropertyModel("wildcardList").getTypeData(), createTypeData(List.class, InterfaceBasedModel.class));
    }

    @Test
    public void testPropertySelection() {
        ClassModel<PropertySelectionModel> classModel = ClassModel.builder(PropertySelectionModel.class).build();

        assertEquals(2, classModel.getPropertyModels().size());
        assertEquals(classModel.getPropertyModel("stringField").getTypeData(), createTypeData(String.class));
        assertEquals(classModel.getPropertyModel("finalStringField").getTypeData(), createTypeData(String.class));
    }

    @Test
    public void testMappingConcreteGenericTypes() {
        ClassModel<?> classModel = ClassModel.builder(NestedGenericHolderMapModel.class).build();

        assertEquals(1, classModel.getPropertyModels().size());
        assertEquals(classModel.getPropertyModels().get(0).getTypeData(), createBuilder(GenericHolderModel.class)
                .addTypeParameter(createTypeData(Map.class, String.class, SimpleModel.class)).build());
    }

    @Test
    public void testAnnotationModel() {
        ClassModel<?> classModel = ClassModel.builder(AnnotationModel.class).build();
        PropertyModel<?> propertyModel = classModel.getIdPropertyModel();

        assertEquals("AnnotationModel", classModel.getName());
        assertEquals(AnnotationModel.class, classModel.getType());
        assertTrue(classModel.useDiscriminator());
        assertEquals("_cls", classModel.getDiscriminatorKey());
        assertEquals("MyAnnotationModel", classModel.getDiscriminator());

        assertEquals(propertyModel, classModel.getPropertyModel("customId"));
        assertTrue(classModel.getInstanceCreatorFactory() instanceof InstanceCreatorFactoryImpl);

        assertEquals(3, classModel.getPropertyModels().size());
        assertEquals(createTypeData(String.class), classModel.getPropertyModel("customId").getTypeData());
        assertEquals(createTypeData(AnnotationModel.class), classModel.getPropertyModel("child").getTypeData());
        assertEquals(createTypeData(AnnotationModel.class), classModel.getPropertyModel("alternative").getTypeData());
    }

    @Test
    public void testInheritedClassAnnotations() {
        ClassModel<?> classModel = ClassModel.builder(AnnotationInheritedModel.class).build();
        assertTrue(classModel.useDiscriminator());
        assertEquals("_cls", classModel.getDiscriminatorKey());
        assertEquals("org.bson.codecs.pojo.entities.conventions.AnnotationInheritedModel", classModel.getDiscriminator());

        assertEquals(2, classModel.getPropertyModels().size());
        assertEquals(createTypeData(String.class), classModel.getPropertyModel("customId").getTypeData());
        assertEquals(createTypeData(AnnotationModel.class), classModel.getPropertyModel("child").getTypeData());

        PropertyModel<?> propertyModel = classModel.getPropertyModel("customId");
        assertEquals(propertyModel, classModel.getIdPropertyModel());

        propertyModel = classModel.getPropertyModel("child");
        assertTrue(propertyModel.useDiscriminator());
    }

    @Test
    public void testOverridePropertyWithSubclass() {
        ClassModel<?> classModel = ClassModel.builder(ShapeHolderModel.class).build();
        assertEquals(1, classModel.getPropertyModels().size());
        assertEquals(createTypeData(ShapeModelAbstract.class), classModel.getPropertyModel("shape").getTypeData());

        ClassModel<?> overriddenClassModel = ClassModel.builder(ShapeHolderCircleModel.class).build();
        assertEquals(1, overriddenClassModel.getPropertyModels().size());
        assertEquals(createTypeData(ShapeModelCircle.class), overriddenClassModel.getPropertyModel("shape").getTypeData());
    }

    @Test
    public void testListGenericExtendedModel() {
        ClassModel<?> classModel = ClassModel.builder(ListGenericExtendedModel.class).build();

        assertEquals(1, classModel.getPropertyModels().size());
        assertEquals(createTypeData(List.class, Integer.class), classModel.getPropertyModel("values").getTypeData());
    }

    @Test
    public void testListListGenericExtendedModel() {
        ClassModel<?> classModel = ClassModel.builder(ListListGenericExtendedModel.class).build();

        assertEquals(1, classModel.getPropertyModels().size());
        assertEquals(createBuilder(List.class).addTypeParameter(createTypeData(List.class, Integer.class)).build(),
                classModel.getPropertyModel("values").getTypeData());
    }

    @Test
    public void testMapGenericExtendedModel() {
        ClassModel<?> classModel = ClassModel.builder(MapGenericExtendedModel.class).build();

        assertEquals(1, classModel.getPropertyModels().size());
        assertEquals(createTypeData(Map.class, String.class, Integer.class), classModel.getPropertyModel("values").getTypeData());
    }

    @Test
    public void testMapMapGenericExtendedModel() {
        ClassModel<?> classModel = ClassModel.builder(MapMapGenericExtendedModel.class).build();

        assertEquals(1, classModel.getPropertyModels().size());
        assertEquals(createBuilder(Map.class).addTypeParameter(createTypeData(String.class))
                        .addTypeParameter(createTypeData(Map.class, String.class, Integer.class)).build(),
                classModel.getPropertyModel("values").getTypeData());
    }

    @Test
    public void testListMapGenericExtendedModel() {
        ClassModel<?> classModel = ClassModel.builder(ListMapGenericExtendedModel.class).build();

        assertEquals(1, classModel.getPropertyModels().size());
        assertEquals(createBuilder(List.class).addTypeParameter(createTypeData(Map.class, String.class, Integer.class)).build(),
                classModel.getPropertyModel("values").getTypeData());
    }


    @Test
    public void testMapListGenericExtendedModel() {
        ClassModel<?> classModel = ClassModel.builder(MapListGenericExtendedModel.class).build();

        assertEquals(1, classModel.getPropertyModels().size());
        assertEquals(createBuilder(Map.class)
                        .addTypeParameter(createTypeData(String.class))
                        .addTypeParameter(createTypeData(List.class, Integer.class)).build(),
                classModel.getPropertyModel("values").getTypeData());
    }


    @Test
    public void testMultipleBoundsModel() {
        ClassModel<?> classModel = ClassModel.builder(MultipleBoundsModel.class).build();

        assertEquals(3, classModel.getPropertyModels().size());

        assertEquals(createTypeData(Double.class), classModel.getPropertyModel("level1").getTypeData());
        assertEquals(createTypeData(List.class, Integer.class), classModel.getPropertyModel("level2").getTypeData());
        assertEquals(createTypeData(Map.class, String.class, String.class), classModel.getPropertyModel("level3").getTypeData());
    }

    @Test
    public void testSimpleWithStaticModel() {
        ClassModel<?> classModel = ClassModel.builder(SimpleWithStaticModel.class).build();

        assertEquals(2, classModel.getPropertyModels().size());
        assertEquals(createTypeData(Integer.class), classModel.getPropertyModel("integerField").getTypeData());
        assertEquals(createTypeData(String.class), classModel.getPropertyModel("stringField").getTypeData());

    }

    <T> TypeData.Builder<T> createBuilder(final Class<T> clazz, final Class<?>... types) {
        TypeData.Builder<T> builder = TypeData.builder(clazz);
        List<TypeData<?>> subTypes = new ArrayList<>();
        for (final Class<?> type : types) {
            subTypes.add(TypeData.builder(type).build());
        }
        builder.addTypeParameters(subTypes);
        return builder;
    }

    <T> TypeData<T> createTypeData(final Class<T> clazz, final Class<?>... types) {
        return createBuilder(clazz, types).build();
    }

}
