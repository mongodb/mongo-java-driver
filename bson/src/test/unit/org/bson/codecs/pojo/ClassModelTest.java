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
import org.bson.codecs.pojo.entities.SimpleGenericsModel;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationInheritedModel;
import org.bson.codecs.pojo.entities.conventions.AnnotationModel;
import org.bson.codecs.pojo.entities.conventions.FieldSelectionModel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public final class ClassModelTest {

    @Test
    @SuppressWarnings("rawtypes")
    public void testSimpleGenericsModel() {
        ClassModel<?> classModel = ClassModel.builder(SimpleGenericsModel.class).build();
        FieldModel<?> fieldModel = classModel.getFieldModels().get(0);

        assertEquals("SimpleGenericsModel", classModel.getName());
        assertEquals(SimpleGenericsModel.class, classModel.getType());
        assertFalse(classModel.useDiscriminator());
        assertEquals("_t", classModel.getDiscriminatorKey());
        assertEquals("SimpleGenericsModel", classModel.getDiscriminator());
        assertNull(classModel.getIdFieldModel());
        assertEquals(4, classModel.getFieldModels().size());
        assertEquals(fieldModel, classModel.getFieldModel(fieldModel.getDocumentFieldName()));
        assertTrue(classModel.getInstanceCreatorFactory() instanceof InstanceCreatorFactoryImpl);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testCollectionNestedPojoModelFieldTypes() {
        TypeData<String> string = TypeData.builder(String.class).build();
        TypeData<SimpleModel> simple = TypeData.builder(SimpleModel.class).build();
        TypeData<ArrayList> list = TypeData.builder(ArrayList.class).addTypeParameter(simple).build();
        TypeData<ArrayList> listList = TypeData.builder(ArrayList.class).addTypeParameter(list).build();
        TypeData<HashSet> set = TypeData.builder(HashSet.class).addTypeParameter(simple).build();
        TypeData<HashSet> setSet = TypeData.builder(HashSet.class).addTypeParameter(set).build();
        TypeData<HashMap> map = TypeData.builder(HashMap.class).addTypeParameter(string).addTypeParameter(simple).build();
        TypeData<ArrayList> listMap = TypeData.builder(ArrayList.class).addTypeParameter(map).build();
        TypeData<HashMap> mapMap = TypeData.builder(HashMap.class).addTypeParameter(string).addTypeParameter(map).build();
        TypeData<HashMap> mapList = TypeData.builder(HashMap.class).addTypeParameter(string).addTypeParameter(list).build();
        TypeData<HashMap> mapListMap = TypeData.builder(HashMap.class).addTypeParameter(string).addTypeParameter(listMap).build();
        TypeData<HashMap> mapSet = TypeData.builder(HashMap.class).addTypeParameter(string).addTypeParameter(set).build();
        TypeData<ArrayList> listMapList = TypeData.builder(ArrayList.class).addTypeParameter(mapList).build();
        TypeData<ArrayList> listMapSet = TypeData.builder(ArrayList.class).addTypeParameter(mapSet).build();


        ClassModel<?> classModel = ClassModel.builder(CollectionNestedPojoModel.class).build();
        assertEquals(list, classModel.getFieldModels().get(0).getTypeData());
        assertEquals(listList, classModel.getFieldModels().get(1).getTypeData());

        assertEquals(set, classModel.getFieldModels().get(2).getTypeData());
        assertEquals(setSet, classModel.getFieldModels().get(3).getTypeData());

        assertEquals(map, classModel.getFieldModels().get(4).getTypeData());
        assertEquals(mapMap, classModel.getFieldModels().get(5).getTypeData());

        assertEquals(mapList, classModel.getFieldModels().get(6).getTypeData());
        assertEquals(mapListMap, classModel.getFieldModels().get(7).getTypeData());
        assertEquals(mapSet, classModel.getFieldModels().get(8).getTypeData());

        assertEquals(listMap, classModel.getFieldModels().get(9).getTypeData());
        assertEquals(listMapList, classModel.getFieldModels().get(10).getTypeData());
        assertEquals(listMapSet, classModel.getFieldModels().get(11).getTypeData());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testMappingConcreteGenericTypes() {
        TypeData<String> string = TypeData.builder(String.class).build();
        TypeData<SimpleModel> simple = TypeData.builder(SimpleModel.class).build();
        TypeData<HashMap> map = TypeData.builder(HashMap.class).addTypeParameter(string).addTypeParameter(simple).build();
        TypeData<GenericHolderModel> genericHolder = TypeData.builder(GenericHolderModel.class).addTypeParameter(map).build();

        ClassModel<?> classModel = ClassModel.builder(NestedGenericHolderMapModel.class).build();
        assertEquals(genericHolder, classModel.getFieldModels().get(0).getTypeData());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testMappingSimpleGenericsModelTypes() {
        TypeData<Object> object = TypeData.builder(Object.class).build();
        TypeData<Integer> integer = TypeData.builder(Integer.class).build();
        TypeData<String> string = TypeData.builder(String.class).build();
        TypeData<ArrayList> list = TypeData.builder(ArrayList.class).addTypeParameter(object).build();
        TypeData<HashMap> map = TypeData.builder(HashMap.class).addTypeParameter(string).addTypeParameter(object).build();

        ClassModel<?> classModel = ClassModel.builder(SimpleGenericsModel.class).build();
        assertEquals(integer, classModel.getFieldModels().get(0).getTypeData());
        assertEquals(object, classModel.getFieldModels().get(1).getTypeData());
        assertEquals(list, classModel.getFieldModels().get(2).getTypeData());
        assertEquals(map, classModel.getFieldModels().get(3).getTypeData());
    }

    @Test
    public void testAnnotationModel() {
        ClassModel<?> classModel = ClassModel.builder(AnnotationModel.class).build();
        FieldModel<?> fieldModel = classModel.getFieldModels().get(0);

        assertEquals("AnnotationModel", classModel.getName());
        assertEquals(AnnotationModel.class, classModel.getType());
        assertTrue(classModel.useDiscriminator());
        assertEquals("_cls", classModel.getDiscriminatorKey());
        assertEquals("MyAnnotationModel", classModel.getDiscriminator());
        assertEquals(fieldModel, classModel.getIdFieldModel());
        assertEquals(3, classModel.getFieldModels().size());
        assertEquals(fieldModel, classModel.getFieldModel(fieldModel.getDocumentFieldName()));
        assertTrue(classModel.getInstanceCreatorFactory() instanceof InstanceCreatorFactoryImpl);
    }

    @Test
    public void testInheritedClassAnnotations() {
        ClassModel<?> classModel = ClassModel.builder(AnnotationInheritedModel.class).build();
        assertTrue(classModel.useDiscriminator());
        assertEquals("_cls", classModel.getDiscriminatorKey());
        assertEquals("AnnotationInheritedModel", classModel.getDiscriminator());

        assertEquals(2, classModel.getFieldModels().size());

        FieldModel<?> fieldModel = classModel.getFieldModel("_id");
        assertEquals(fieldModel, classModel.getIdFieldModel());
        assertEquals(fieldModel, classModel.getFieldModel(fieldModel.getDocumentFieldName()));

        fieldModel = classModel.getFieldModel("child");
        assertTrue(fieldModel.useDiscriminator());
    }

    @Test
    public void testFieldSelection() {
        ClassModel<?> classModel = ClassModel.builder(FieldSelectionModel.class).build();

        assertEquals(2, classModel.getFieldModels().size());
        assertEquals("myFinalField", classModel.getFieldModels().get(0).getFieldName());
        assertEquals("myIntField", classModel.getFieldModels().get(1).getFieldName());
    }

}
