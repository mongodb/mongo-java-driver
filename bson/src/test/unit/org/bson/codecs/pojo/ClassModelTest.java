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

import org.bson.codecs.pojo.entities.CollectionNestedPojoModel;
import org.bson.codecs.pojo.entities.GenericHolderModel;
import org.bson.codecs.pojo.entities.NestedGenericHolderMapModel;
import org.bson.codecs.pojo.entities.PropertySelectionModel;
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationInheritedModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationModel;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public final class ClassModelTest {

    @Test
    @SuppressWarnings("rawtypes")
    public void testSimpleGenericsModel() {
        ClassModel<?> classModel = ClassModel.builder(SimpleGenericsModel.class).build();

        assertEquals("SimpleGenericsModel", classModel.getName());
        assertEquals(SimpleGenericsModel.class, classModel.getType());
        assertFalse(classModel.useDiscriminator());
        assertEquals("_t", classModel.getDiscriminatorKey());
        assertEquals("org.bson.codecs.pojo.entities.SimpleGenericsModel", classModel.getDiscriminator());
        assertNull(classModel.getIdPropertyModel());
        assertEquals(4, classModel.getPropertyModels().size());
        assertTrue(classModel.getInstanceCreatorFactory() instanceof InstanceCreatorFactoryImpl);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testCollectionNestedPojoModelPropertyTypes() {
        TypeData<String> string = TypeData.builder(String.class).build();
        TypeData<SimpleModel> simple = TypeData.builder(SimpleModel.class).build();
        TypeData<List> list = TypeData.builder(List.class).addTypeParameter(simple).build();
        TypeData<List> listList = TypeData.builder(List.class).addTypeParameter(list).build();
        TypeData<Set> set = TypeData.builder(Set.class).addTypeParameter(simple).build();
        TypeData<Set> setSet = TypeData.builder(Set.class).addTypeParameter(set).build();
        TypeData<Map> map = TypeData.builder(Map.class).addTypeParameter(string).addTypeParameter(simple).build();
        TypeData<List> listMap = TypeData.builder(List.class).addTypeParameter(map).build();
        TypeData<Map> mapMap = TypeData.builder(Map.class).addTypeParameter(string).addTypeParameter(map).build();
        TypeData<Map> mapList = TypeData.builder(Map.class).addTypeParameter(string).addTypeParameter(list).build();
        TypeData<Map> mapListMap = TypeData.builder(Map.class).addTypeParameter(string).addTypeParameter(listMap).build();
        TypeData<Map> mapSet = TypeData.builder(Map.class).addTypeParameter(string).addTypeParameter(set).build();
        TypeData<List> listMapList = TypeData.builder(List.class).addTypeParameter(mapList).build();
        TypeData<List> listMapSet = TypeData.builder(List.class).addTypeParameter(mapSet).build();

        ClassModel<?> classModel = ClassModel.builder(CollectionNestedPojoModel.class).build();
        assertEquals(list, classModel.getPropertyModel("listSimple").getTypeData());
        assertEquals(listList, classModel.getPropertyModel("listListSimple").getTypeData());

        assertEquals(set, classModel.getPropertyModel("setSimple").getTypeData());
        assertEquals(setSet, classModel.getPropertyModel("setSetSimple").getTypeData());

        assertEquals(map, classModel.getPropertyModel("mapSimple").getTypeData());
        assertEquals(mapMap, classModel.getPropertyModel("mapMapSimple").getTypeData());

        assertEquals(mapList, classModel.getPropertyModel("mapListSimple").getTypeData());
        assertEquals(mapListMap, classModel.getPropertyModel("mapListMapSimple").getTypeData());
        assertEquals(mapSet, classModel.getPropertyModel("mapSetSimple").getTypeData());

        assertEquals(listMap, classModel.getPropertyModel("listMapSimple").getTypeData());
        assertEquals(listMapList, classModel.getPropertyModel("listMapListSimple").getTypeData());
        assertEquals(listMapSet, classModel.getPropertyModel("listMapSetSimple").getTypeData());
    }

    @Test
    public void testPropertySelection() {
        ClassModel<PropertySelectionModel> classModel = ClassModel.builder(PropertySelectionModel.class).build();

        assertEquals(2, classModel.getPropertyModels().size());
        assertNotNull(classModel.getPropertyModel("stringField"));
        assertNotNull(classModel.getPropertyModel("finalStringField"));
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testMappingConcreteGenericTypes() {
        TypeData<String> string = TypeData.builder(String.class).build();
        TypeData<SimpleModel> simple = TypeData.builder(SimpleModel.class).build();
        TypeData<Map> map = TypeData.builder(Map.class).addTypeParameter(string).addTypeParameter(simple).build();
        TypeData<GenericHolderModel> genericHolder = TypeData.builder(GenericHolderModel.class).addTypeParameter(map).build();

        ClassModel<?> classModel = ClassModel.builder(NestedGenericHolderMapModel.class).build();
        assertEquals(genericHolder, classModel.getPropertyModels().get(0).getTypeData());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testMappingSimpleGenericsModelTypes() {
        TypeData<Object> object = TypeData.builder(Object.class).build();
        TypeData<Integer> integer = TypeData.builder(Integer.class).build();
        TypeData<String> string = TypeData.builder(String.class).build();
        TypeData<List> list = TypeData.builder(List.class).addTypeParameter(object).build();
        TypeData<Map> map = TypeData.builder(Map.class).addTypeParameter(string).addTypeParameter(object).build();

        ClassModel<?> classModel = ClassModel.builder(SimpleGenericsModel.class).build();
        assertEquals(integer, classModel.getPropertyModel("myIntegerField").getTypeData());
        assertEquals(object, classModel.getPropertyModel("myGenericField").getTypeData());
        assertEquals(list, classModel.getPropertyModel("myListField").getTypeData());
        assertEquals(map, classModel.getPropertyModel("myMapField").getTypeData());
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
        assertEquals(propertyModel, classModel.getIdPropertyModel());
        assertEquals(3, classModel.getPropertyModels().size());
        assertEquals(propertyModel, classModel.getPropertyModel("customId"));
        assertTrue(classModel.getInstanceCreatorFactory() instanceof InstanceCreatorFactoryImpl);
    }

    @Test
    public void testInheritedClassAnnotations() {
        ClassModel<?> classModel = ClassModel.builder(AnnotationInheritedModel.class).build();
        assertTrue(classModel.useDiscriminator());
        assertEquals("_cls", classModel.getDiscriminatorKey());
        assertEquals("org.bson.codecs.pojo.entities.conventions.AnnotationInheritedModel", classModel.getDiscriminator());

        assertEquals(2, classModel.getPropertyModels().size());

        PropertyModel<?> propertyModel = classModel.getPropertyModel("customId");
        assertEquals(propertyModel, classModel.getIdPropertyModel());

        propertyModel = classModel.getPropertyModel("child");
        assertTrue(propertyModel.useDiscriminator());
    }

}
