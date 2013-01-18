/**
 *
 */
package com.google.code.morphia.mapping.validation.classrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Version;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class MultipleVersionsTest extends TestBase {

    static class Fail1 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Version
        private long v1;
        @Version
        private long v2;
    }

    static class OK1 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Version
        private long v1;
    }

    @Test
    public void testCheck() {
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() throws Throwable {
                morphia.map(Fail1.class);
            }
        };
        morphia.map(OK1.class);
    }

}
