package com.google.code.morphia.issue50;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

import static org.junit.Assert.fail;

public class IdTwiceTest extends TestBase {

    @Test
    public final void testRedundantId() {
        try {
            morphia.map(A.class);
            fail();
        } catch (ConstraintViolationException expected) {
            // fine
        }
    }

    static class A extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Id
        private String extraId;
        @Id
        private String broken;
    }

}
