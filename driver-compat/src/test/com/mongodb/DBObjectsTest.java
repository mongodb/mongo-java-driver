/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.mongodb;

import org.bson.types.Document;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DBObjectsTest {
    @Test
    public void shouldCreateADocumentWithTheSameSimpleValuesFromADBObject() {
        final DBObject dbObject = createBasicDBObject();

        final Document actualDocument = DBObjects.toDocument(dbObject);

        final Document expectedDocument = createDocument();

        assertThat(actualDocument, is(expectedDocument));
    }

    @Test
    public void shouldCreateADBObjectWithTheSameSimpleValuesFromADocument() {
        final Document document = createDocument();

        final DBObject actualDBObject = DBObjects.toDBObject(document);

        final DBObject expectedDBObject = createBasicDBObject();

        assertThat(actualDBObject, is(expectedDBObject));
    }

    @Test
    public void shouldHandleNestedDocumentsWhenConvertingDocumentToDBObject() {
        final BasicDBObject dbObject = createBasicDBObject();
        dbObject.append("subDocument", createBasicDBObject());

        final Document actualDocument = DBObjects.toDocument(dbObject);

        final Document expectedDocument = createDocument();
        expectedDocument.append("subDocument", createDocument());

        assertThat(actualDocument, is(expectedDocument));
    }

    @Test
    public void shouldHandleNestedDocumentsWhenConvertingDBObjectToDocument() {
        final Document document = createDocument();
        document.append("subDocument", createDocument());

        final BasicDBObject actualDBObject = DBObjects.toDBObject(document);

        final BasicDBObject expectedDBObject = createBasicDBObject();
        expectedDBObject.append("subDocument", createBasicDBObject());

        assertThat(actualDBObject, is(expectedDBObject));
    }

    @Test
    public void shouldHandleComplexTypesWhenConvertingDBObjectToDocument() {
        final BasicDBObject dbObject = createBasicDBObject();
        dbObject.append("complexType", createNewTestObject());

        final Document actualDocument = DBObjects.toDocument(dbObject);

        final Document expectedDocument = createDocument();
        expectedDocument.append("complexType", createNewTestObject());

        assertThat(actualDocument, is(expectedDocument));
    }

    @Test
    public void shouldHandleComplexTypesWhenConvertingDocumentToDBObject() {
        final MyTestObject testObject = createNewTestObject();

        final Document document = createDocument();
        document.append("complexType", testObject);

        final BasicDBObject actualDBObject = DBObjects.toDBObject(document);

        final BasicDBObject expectedDBObject = createBasicDBObject();
        expectedDBObject.append("complexType", createNewTestObject());

        assertThat(actualDBObject, is(expectedDBObject));
    }

    @Test
    public void shouldHandleNullsWhenConvertingDBObjectToDocument() {
        final BasicDBObject dbObject = createBasicDBObject();
        dbObject.append("myNullValue", null);

        final Document actualDocument = DBObjects.toDocument(dbObject);

        final Document expectedDocument = createDocument();
        expectedDocument.append("myNullValue", null);

        assertThat(actualDocument, is(expectedDocument));
    }

    @Test
    public void shouldHandleNullsWhenConvertingDocumentToDBObject() {
        final Document document = createDocument();
        document.append("myNullValue", null);

        final BasicDBObject actualDBObject = DBObjects.toDBObject(document);

        final BasicDBObject expectedDBObject = createBasicDBObject();
        expectedDBObject.append("myNullValue", null);

        assertThat(actualDBObject, is(expectedDBObject));
    }

    @Test
    public void shouldHandleListsWhenConvertingDBObjectToDocument() {
        final BasicDBObject dbObject = createBasicDBObject();
        dbObject.append("listOfInts", asList(7843, 75439, 57489, 547));

        final Document actualDocument = DBObjects.toDocument(dbObject);

        final Document expectedDocument = createDocument();
        expectedDocument.append("listOfInts", asList(7843, 75439, 57489, 547));

        assertThat(actualDocument, is(expectedDocument));
    }

    @Test
    public void shouldHandleListsWhenConvertingDocumentToDBObject() {
        final Document document = createDocument();
        document.append("listOfInts", asList(7843, 75439, 57489, 547));

        final BasicDBObject actualDBObject = DBObjects.toDBObject(document);

        final BasicDBObject expectedDBObject = createBasicDBObject();
        expectedDBObject.append("listOfInts", asList(7843, 75439, 57489, 547));

        assertThat(actualDBObject, is(expectedDBObject));
    }

    //TODO: tests around recursive structures

    private BasicDBObject createBasicDBObject() {
        BasicDBObject dbObject = new BasicDBObject();
        dbObject.append("IntValue", 8563838);
        dbObject.append("StringValue", "Something");
        dbObject.append("LongValue", 5748932L);
        dbObject.append("DoubleValue", 3.14);
        return dbObject;
    }

    private Document createDocument() {
        Document document = new Document();
        document.append("IntValue", 8563838);
        document.append("StringValue", "Something");
        document.append("LongValue", 5748932L);
        document.append("DoubleValue", 3.14);
        return document;
    }

    private MyTestObject createNewTestObject() {
        final MyTestObject testObject = new MyTestObject();
        testObject.myStringField = "I'm a String";
        testObject.myIntField = 45;
        testObject.myDoubleField = 837.8675;
        return testObject;
    }

    private class MyTestObject {
        private String myStringField;
        private int myIntField;
        private double myDoubleField;
        private MyTestObject myTestObject;

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final MyTestObject that = (MyTestObject) o;

            if (Double.compare(that.myDoubleField, myDoubleField) != 0) {
                return false;
            }
            if (myIntField != that.myIntField) {
                return false;
            }
            if (!myStringField.equals(that.myStringField)) {
                return false;
            }
            if (myTestObject != null ? !myTestObject.equals(that.myTestObject) : that.myTestObject != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = myStringField.hashCode();
            result = 31 * result + myIntField;
            temp = myDoubleField != +0.0d ? Double.doubleToLongBits(myDoubleField) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            result = 31 * result + (myTestObject != null ? myTestObject.hashCode() : 0);
            return result;
        }
    }

}
