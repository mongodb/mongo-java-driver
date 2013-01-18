/**
 *
 */
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@SuppressWarnings("unchecked")
public class MapKeyDifferentFromStringTest extends TestBase {

    static class MapWithWrongKeyType1 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Serialized
        private final Map<Integer, Integer> shouldBeOk = new HashMap();

    }

    static class MapWithWrongKeyType2 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Reference
        private final Map<Integer, Integer> shouldBeOk = new HashMap();

    }

    static class MapWithWrongKeyType3 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Embedded
        private final Map<BigDecimal, Integer> shouldBeOk = new HashMap();

    }

    @Test
    public void testCheck() {
        morphia.map(MapWithWrongKeyType1.class);

        new AssertedFailure() {
            public void thisMustFail() throws Throwable {
                morphia.map(MapWithWrongKeyType2.class);
            }
        };

        new AssertedFailure() {
            public void thisMustFail() throws Throwable {
                morphia.map(MapWithWrongKeyType3.class);
            }
        };
    }

}
