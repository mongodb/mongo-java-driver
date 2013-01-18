/**
 *
 */
package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedList;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ClassMappingTest extends TestBase {

    @SuppressWarnings("unchecked")
    public static class E {
        @Id
        private ObjectId id;

        @Property
        private Class<? extends Collection> testClass;
        private Class<? extends Collection> testClass2;
    }

    @Test
    public void testMapping() throws Exception {
        E e = new E();

        e.testClass = LinkedList.class;
        ds.save(e);

        e = ds.get(e);
        Assert.assertEquals(LinkedList.class, e.testClass);
    }

    @Test
    public void testMappingWithoutAnnotation() throws Exception {
        E e = new E();

        e.testClass2 = LinkedList.class;
        ds.save(e);

        e = ds.get(e);
        Assert.assertEquals(LinkedList.class, e.testClass2);
    }

}
