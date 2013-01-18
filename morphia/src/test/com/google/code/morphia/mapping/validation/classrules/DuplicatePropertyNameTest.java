/**
 *
 */
package com.google.code.morphia.mapping.validation.classrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.AssertedFailure;
import org.junit.Test;

import java.util.Map;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class DuplicatePropertyNameTest extends TestBase {
    @Entity
    static class DuplicatedPropertyName {
        @Id
        private String id;

        @Property(value = "value")
        private String content1;
        @Property(value = "value")
        private String content2;
    }

    @Entity
    static class DuplicatedPropertyName2 {
        @Id
        private String id;

        @Embedded(value = "value")
        private Map<String, Integer> content1;
        @Property(value = "value")
        private String content2;
    }

    @Entity
    static class Super {
        private String foo;
    }

    static class Extends extends Super {
        private String foo;
    }

    @Test
    public void testDuplicatedPropertyName() throws Exception {
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() throws Throwable {
                morphia.map(DuplicatedPropertyName.class);
            }
        };
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() throws Throwable {
                morphia.map(DuplicatedPropertyName2.class);
            }
        };
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() throws Throwable {
                morphia.map(Extends.class);
            }
        };
    }

}
