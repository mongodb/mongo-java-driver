/**
 *
 */
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ReferenceAndSerializableTest extends TestBase {
    static class E extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Reference
        @Serialized
        private R r;
    }

    static class R extends TestEntity {
        private static final long serialVersionUID = 1L;
    }

    @Test
    public void testCheck() {
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() throws Throwable {
                morphia.map(E.class);
            }

            @Override
            protected boolean dumpToSystemOut() {
                return true;
            }
        };
    }
}
