/**
 *
 */
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class LazyReferenceOnArrayTest extends TestBase {

    static class LazyOnArray extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Reference(lazy = true)
        private R[] r;
    }

    static class R extends TestEntity {
        private static final long serialVersionUID = 1L;
    }

    @Test
    public void testLazyRefOnArray() {
        new AssertedFailure(ConstraintViolationException.class) {

            @Override
            protected void thisMustFail() throws Throwable {
                morphia.map(LazyOnArray.class);
            }
        };
    }
}
