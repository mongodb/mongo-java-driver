/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.types;

import org.junit.Test;

import java.math.BigDecimal;

import static org.bson.types.Decimal128.NEGATIVE_INFINITY;
import static org.bson.types.Decimal128.NEGATIVE_NaN;
import static org.bson.types.Decimal128.NEGATIVE_ZERO;
import static org.bson.types.Decimal128.NaN;
import static org.bson.types.Decimal128.POSITIVE_INFINITY;
import static org.bson.types.Decimal128.POSITIVE_ZERO;
import static org.bson.types.Decimal128.fromIEEE754BIDEncoding;
import static org.bson.types.Decimal128.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Decimal128Test {

    @Test
    public void shouldHaveCorrectConstants() {
        // expect
        assertEquals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000000L), POSITIVE_ZERO);
        assertEquals(fromIEEE754BIDEncoding(0xb040000000000000L, 0x0000000000000000L), NEGATIVE_ZERO);
        assertEquals(fromIEEE754BIDEncoding(0x7800000000000000L, 0x0000000000000000L), POSITIVE_INFINITY);
        assertEquals(fromIEEE754BIDEncoding(0xf800000000000000L, 0x0000000000000000L), NEGATIVE_INFINITY);
        assertEquals(fromIEEE754BIDEncoding(0x7c00000000000000L, 0x0000000000000000L), NaN);
    }

    @Test
    public void shouldConstructFromHighAndLow() {
        // given
        Decimal128 val = fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L);

        // then
        assertEquals(0x3040000000000000L, val.getHigh());
        assertEquals(0x0000000000000001L, val.getLow());
    }

    @Test
    public void shouldConstructFromSimpleString() {
        // expect
        assertEquals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000000L), parse("0"));
        assertEquals(fromIEEE754BIDEncoding(0xb040000000000000L, 0x0000000000000000L), parse("-0"));
        assertEquals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L), parse("1"));
        assertEquals(fromIEEE754BIDEncoding(0xb040000000000000L, 0x0000000000000001L), parse("-1"));
        assertEquals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x002bdc545d6b4b87L), parse("12345678901234567"));
        assertEquals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x000000e67a93c822L), parse("989898983458"));
        assertEquals(fromIEEE754BIDEncoding(0xb040000000000000L, 0x002bdc545d6b4b87L), parse("-12345678901234567"));
        assertEquals(fromIEEE754BIDEncoding(0x3036000000000000L, 0x0000000000003039L), parse("0.12345"));
        assertEquals(fromIEEE754BIDEncoding(0x3032000000000000L, 0x0000000000003039L), parse("0.0012345"));
        assertEquals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x002bdc545d6b4b87L), parse("00012345678901234567"));
    }

    @Test
    public void shouldRoundExactly() {
        // expect
        assertEquals(parse("1.234567890123456789012345678901234"), parse("1.234567890123456789012345678901234"));
        assertEquals(parse("1.234567890123456789012345678901234"), parse("1.2345678901234567890123456789012340"));
        assertEquals(parse("1.234567890123456789012345678901234"), parse("1.23456789012345678901234567890123400"));
        assertEquals(parse("1.234567890123456789012345678901234"), parse("1.234567890123456789012345678901234000"));
    }

    @Test
    public void shouldClampPositiveExponents() {
        // expect
        assertEquals(parse("10E6111"), parse("1E6112"));
        assertEquals(parse("100E6111"), parse("1E6113"));
        assertEquals(parse("100000000000000000000000000000000E+6111"), parse("1E6143"));
        assertEquals(parse("1000000000000000000000000000000000E+6111"), parse("1E6144"));
        assertEquals(parse("1100000000000000000000000000000000E+6111"), parse("11E6143"));
        assertEquals(parse("0E6111"), parse("0E8000"));
        assertEquals(parse("0E6111"), parse("0E2147483647"));

        assertEquals(parse("-10E6111"), parse("-1E6112"));
        assertEquals(parse("-100E6111"), parse("-1E6113"));
        assertEquals(parse("-100000000000000000000000000000000E+6111"), parse("-1E6143"));
        assertEquals(parse("-1000000000000000000000000000000000E+6111"), parse("-1E6144"));
        assertEquals(parse("-1100000000000000000000000000000000E+6111"), parse("-11E6143"));
        assertEquals(parse("-0E6111"), parse("-0E8000"));
        assertEquals(parse("-0E6111"), parse("-0E2147483647"));
    }

    @Test
    public void shouldClampNegativeExponents() {
        // expect
        assertEquals(parse("0E-6176"), parse("0E-8000"));
        assertEquals(parse("0E-6176"), parse("0E-2147483647"));
        assertEquals(parse("1E-6176"), parse("10E-6177"));
        assertEquals(parse("1E-6176"), parse("100E-6178"));
        assertEquals(parse("11E-6176"), parse("110E-6177"));

        assertEquals(parse("-0E-6176"), parse("-0E-8000"));
        assertEquals(parse("-0E-6176"), parse("-0E-2147483647"));
        assertEquals(parse("-1E-6176"), parse("-10E-6177"));
        assertEquals(parse("-1E-6176"), parse("-100E-6178"));
        assertEquals(parse("-11E-6176"), parse("-110E-6177"));
    }

    @Test
    public void shouldConstructFromLong() {
        // expect
        assertEquals(new Decimal128(new BigDecimal("1")), new Decimal128(1L));
        assertEquals(new Decimal128(new BigDecimal(Long.MIN_VALUE)), new Decimal128(Long.MIN_VALUE));
        assertEquals(new Decimal128(new BigDecimal(Long.MAX_VALUE)), new Decimal128(Long.MAX_VALUE));
    }

    @Test
    public void shouldConstructFromLargeBigDecimal() {
        // expect
        assertEquals(fromIEEE754BIDEncoding(0x304000000000029dL, 0x42da3a76f9e0d979L), parse("12345689012345789012345"));
        assertEquals(fromIEEE754BIDEncoding(0x30403cde6fff9732L, 0xde825cd07e96aff2L), parse("1234567890123456789012345678901234"));
        assertEquals(fromIEEE754BIDEncoding(0x5fffed09bead87c0L, 0x378d8e63ffffffffL), parse("9.999999999999999999999999999999999E+6144"));
        assertEquals(fromIEEE754BIDEncoding(0x0001ed09bead87c0L, 0x378d8e63ffffffffL), parse("9.999999999999999999999999999999999E-6143"));
        assertEquals(fromIEEE754BIDEncoding(0x3040ffffffffffffL, 0xffffffffffffffffL), parse("5.192296858534827628530496329220095E+33"));
    }

    @Test
    public void shouldConvertToSimpleBigDecimal() {
        // expect
        assertEquals(new BigDecimal("0"), fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000000L).bigDecimalValue());
        assertEquals(new BigDecimal("1"), fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L).bigDecimalValue());
        assertEquals(new BigDecimal("-1"), fromIEEE754BIDEncoding(0xb040000000000000L, 0x0000000000000001L).bigDecimalValue());
        assertEquals(new BigDecimal("12345678901234567"),
                fromIEEE754BIDEncoding(0x3040000000000000L, 0x002bdc545d6b4b87L).bigDecimalValue());
        assertEquals(new BigDecimal("989898983458"), fromIEEE754BIDEncoding(0x3040000000000000L, 0x000000e67a93c822L).bigDecimalValue());
        assertEquals(new BigDecimal("-12345678901234567"),
                fromIEEE754BIDEncoding(0xb040000000000000L, 0x002bdc545d6b4b87L).bigDecimalValue());
        assertEquals(new BigDecimal("0.12345"), fromIEEE754BIDEncoding(0x3036000000000000L, 0x0000000000003039L).bigDecimalValue());
        assertEquals(new BigDecimal("0.0012345"), fromIEEE754BIDEncoding(0x3032000000000000L, 0x0000000000003039L).bigDecimalValue());
        assertEquals(new BigDecimal("00012345678901234567"),
                fromIEEE754BIDEncoding(0x3040000000000000L, 0x002bdc545d6b4b87L).bigDecimalValue());
    }

    @Test
    public void shouldConvertToLargeBigDecimal() {
        // expect
        assertEquals(new BigDecimal("12345689012345789012345"),
                fromIEEE754BIDEncoding(0x304000000000029dL, 0x42da3a76f9e0d979L).bigDecimalValue());

        assertEquals(new BigDecimal("1234567890123456789012345678901234"), fromIEEE754BIDEncoding(0x30403cde6fff9732L,
                0xde825cd07e96aff2L).bigDecimalValue());

        assertEquals(new BigDecimal("9.999999999999999999999999999999999E+6144"),
                fromIEEE754BIDEncoding(0x5fffed09bead87c0L, 0x378d8e63ffffffffL).bigDecimalValue());

        assertEquals(new BigDecimal("9.999999999999999999999999999999999E-6143"),
                fromIEEE754BIDEncoding(0x0001ed09bead87c0L, 0x378d8e63ffffffffL).bigDecimalValue());

        assertEquals(new BigDecimal("5.192296858534827628530496329220095E+33"),
                fromIEEE754BIDEncoding(0x3040ffffffffffffL, 0xffffffffffffffffL).bigDecimalValue());
    }

    @Test
    public void shouldConvertInvalidRepresentationsOfZeroAsBigDecimalZero() {
        // expect
        assertEquals(new BigDecimal("0"), fromIEEE754BIDEncoding(0x6C10000000000000L, 0x0).bigDecimalValue());
        assertEquals(new BigDecimal("0E+3"), fromIEEE754BIDEncoding(0x6C11FFFFFFFFFFFFL, 0xffffffffffffffffL).bigDecimalValue());
    }

    @Test
    public void shouldDetectInfinity() {
        // expect
        assertTrue(POSITIVE_INFINITY.isInfinite());
        assertTrue(NEGATIVE_INFINITY.isInfinite());
        assertFalse(parse("0").isInfinite());
        assertFalse(parse("9.999999999999999999999999999999999E+6144").isInfinite());
        assertFalse(parse("9.999999999999999999999999999999999E-6143").isInfinite());
        assertFalse(POSITIVE_INFINITY.isFinite());
        assertFalse(NEGATIVE_INFINITY.isFinite());
        assertTrue(parse("0").isFinite());
        assertTrue(parse("9.999999999999999999999999999999999E+6144").isFinite());
        assertTrue(parse("9.999999999999999999999999999999999E-6143").isFinite());
    }

    @Test
    public void shouldDetectNaN() {
        // expect
        assertTrue(NaN.isNaN());
        assertTrue(fromIEEE754BIDEncoding(0x7e00000000000000L, 0).isNaN());    // SNaN
        assertFalse(POSITIVE_INFINITY.isNaN());
        assertFalse(NEGATIVE_INFINITY.isNaN());
        assertFalse(parse("0").isNaN());
        assertFalse(parse("9.999999999999999999999999999999999E+6144").isNaN());
        assertFalse(parse("9.999999999999999999999999999999999E-6143").isNaN());
    }

    @Test
    public void shouldConvertNaNToString() {
        // expect
        assertEquals("NaN", NaN.toString());
    }

    @Test
    public void shouldConvertNaNFromString() {
        // expect
        assertEquals(NaN, parse("NaN"));
        assertEquals(NaN, parse("nan"));
        assertEquals(NaN, parse("nAn"));
        assertEquals(NEGATIVE_NaN, parse("-NaN"));
        assertEquals(NEGATIVE_NaN, parse("-nan"));
        assertEquals(NEGATIVE_NaN, parse("-nAn"));
    }

    @Test(expected = ArithmeticException.class)
    public void shouldNotConvertNaNToBigDecimal() {
        // when
        NaN.bigDecimalValue();
    }

    @Test
    public void shouldConvertInfinityToString() {
        // expect
        assertEquals("Infinity", POSITIVE_INFINITY.toString());
        assertEquals("-Infinity", NEGATIVE_INFINITY.toString());
    }

    @Test
    public void shouldConvertInfinityFromString() {
        // expect
        assertEquals(POSITIVE_INFINITY, parse("Inf"));
        assertEquals(POSITIVE_INFINITY, parse("inf"));
        assertEquals(POSITIVE_INFINITY, parse("inF"));
        assertEquals(POSITIVE_INFINITY, parse("+Inf"));
        assertEquals(POSITIVE_INFINITY, parse("+inf"));
        assertEquals(POSITIVE_INFINITY, parse("+inF"));
        assertEquals(POSITIVE_INFINITY, parse("Infinity"));
        assertEquals(POSITIVE_INFINITY, parse("infinity"));
        assertEquals(POSITIVE_INFINITY, parse("infiniTy"));
        assertEquals(POSITIVE_INFINITY, parse("+Infinity"));
        assertEquals(POSITIVE_INFINITY, parse("+infinity"));
        assertEquals(POSITIVE_INFINITY, parse("+infiniTy"));
        assertEquals(NEGATIVE_INFINITY, parse("-Inf"));
        assertEquals(NEGATIVE_INFINITY, parse("-inf"));
        assertEquals(NEGATIVE_INFINITY, parse("-inF"));
        assertEquals(NEGATIVE_INFINITY, parse("-Infinity"));
        assertEquals(NEGATIVE_INFINITY, parse("-infinity"));
        assertEquals(NEGATIVE_INFINITY, parse("-infiniTy"));
    }

    @Test
    public void shouldConvertFiniteToString() {
        // expect
        assertEquals("0", parse("0").toString());
        assertEquals("-0", parse("-0").toString());
        assertEquals("0E+10", parse("0E10").toString());
        assertEquals("-0E+10", parse("-0E10").toString());
        assertEquals("1", parse("1").toString());
        assertEquals("-1", parse("-1").toString());
        assertEquals("-1.1", parse("-1.1").toString());

        assertEquals("1.23E-7", parse("123E-9").toString());
        assertEquals("0.00000123", parse("123E-8").toString());
        assertEquals("0.0000123", parse("123E-7").toString());
        assertEquals("0.000123", parse("123E-6").toString());
        assertEquals("0.00123", parse("123E-5").toString());
        assertEquals("0.0123", parse("123E-4").toString());
        assertEquals("0.123", parse("123E-3").toString());
        assertEquals("1.23", parse("123E-2").toString());
        assertEquals("12.3", parse("123E-1").toString());
        assertEquals("123", parse("123E0").toString());
        assertEquals("1.23E+3", parse("123E1").toString());

        assertEquals("0.0001234", parse("1234E-7").toString());
        assertEquals("0.001234", parse("1234E-6").toString());

        assertEquals("1E+6", parse("1E6").toString());
    }

    @Test
    public void shouldConvertInvalidRepresentationsOfZeroToString() {
        // expect
        assertEquals("0", fromIEEE754BIDEncoding(0x6C10000000000000L, 0x0).bigDecimalValue().toString());
        assertEquals("0E+3", fromIEEE754BIDEncoding(0x6C11FFFFFFFFFFFFL, 0xffffffffffffffffL).toString());
    }

    @Test
    public void testEquals() {
        // given
        Decimal128 d1 = fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L);
        Decimal128 d2 = fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L);
        Decimal128 d3 = fromIEEE754BIDEncoding(0x3040000000000001L, 0x0000000000000001L);
        Decimal128 d4 = fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000011L);

        // expect
        assertTrue(d1.equals(d1));
        assertTrue(d1.equals(d2));
        assertFalse(d1.equals(d3));
        assertFalse(d1.equals(d4));
        assertFalse(d1.equals(null));
        assertFalse(d1.equals(0L));
    }

    @Test
    public void testHashCode() {
        // expect
        assertEquals(809500703, fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L).hashCode());
    }

    @Test(expected = ArithmeticException.class)
    public void shouldNotConvertPositiveInfinityToBigDecimal() {
        POSITIVE_INFINITY.bigDecimalValue();
    }

    @Test(expected = ArithmeticException.class)
    public void shouldNotConvertNegativeInfinityToBigDecimal() {
        NEGATIVE_INFINITY.bigDecimalValue();
    }

    @Test
    public void shouldNotConvertNegativeZeroToBigDecimal() {
        try {
            parse("-0").bigDecimalValue();
            fail();
        } catch (ArithmeticException e) {
            // pass
        }

        try {
            parse("-0E+1").bigDecimalValue();
            fail();
        } catch (ArithmeticException e) {
            // pass
        }

        try {
            parse("-0E-1").bigDecimalValue();
            fail();
        } catch (ArithmeticException e) {
            // pass
        }
    }

    @Test
    public void shouldNotRoundInexactly() {
        try {
            parse("12345678901234567890123456789012345E+6111");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("123456789012345678901234567890123456E+6111");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("1234567890123456789012345678901234567E+6111");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("12345678901234567890123456789012345E-6176");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("123456789012345678901234567890123456E-6176");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("1234567890123456789012345678901234567E-6176");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-12345678901234567890123456789012345E+6111");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-123456789012345678901234567890123456E+6111");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-1234567890123456789012345678901234567E+6111");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-12345678901234567890123456789012345E-6176");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-123456789012345678901234567890123456E-6176");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-1234567890123456789012345678901234567E-6176");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    @Test
    public void shouldNotClampLargeExponentsIfNoExtraPrecisionIsAvailable() {
        try {
            parse("1234567890123456789012345678901234E+6112");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("1234567890123456789012345678901234E+6113");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("1234567890123456789012345678901234E+6114");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-1234567890123456789012345678901234E+6112");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-1234567890123456789012345678901234E+6113");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-1234567890123456789012345678901234E+6114");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    @Test
    public void shouldNotClampSmallExponentsIfNoExtraPrecisionCanBeDiscarded() {
        try {
            parse("1234567890123456789012345678901234E-6177");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("1234567890123456789012345678901234E-6178");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("1234567890123456789012345678901234E-6179");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-1234567890123456789012345678901234E-6177");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-1234567890123456789012345678901234E-6178");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
        try {
            parse("-1234567890123456789012345678901234E-6179");
            fail();
        } catch (IllegalArgumentException e) {
            // pass
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfBigDecimalIsTooLarge() {
        new Decimal128(new BigDecimal("12345678901234567890123456789012345"));
    }

    @Test
    public void shouldExtendNumber() {
        // expect
        assertEquals(Double.POSITIVE_INFINITY, POSITIVE_INFINITY.doubleValue(), 0);
        assertEquals(Float.POSITIVE_INFINITY, POSITIVE_INFINITY.floatValue(), 0);
        assertEquals(Long.MAX_VALUE, POSITIVE_INFINITY.longValue());
        assertEquals(Integer.MAX_VALUE, POSITIVE_INFINITY.intValue());

        assertEquals(Double.NEGATIVE_INFINITY, NEGATIVE_INFINITY.doubleValue(), 0);
        assertEquals(Float.NEGATIVE_INFINITY, NEGATIVE_INFINITY.floatValue(), 0);
        assertEquals(Long.MIN_VALUE, NEGATIVE_INFINITY.longValue());
        assertEquals(Integer.MIN_VALUE, NEGATIVE_INFINITY.intValue());

        assertEquals(Double.NaN, NaN.doubleValue(), 0);
        assertEquals(Double.NaN, NaN.floatValue(), 0);
        assertEquals(0, NaN.longValue());
        assertEquals(0, NaN.intValue());

        assertEquals(Double.NaN, NEGATIVE_NaN.doubleValue(), 0);
        assertEquals(Float.NaN, NEGATIVE_NaN.floatValue(), 0);
        assertEquals(0, NEGATIVE_NaN.longValue());
        assertEquals(0, NEGATIVE_NaN.intValue());

        assertEquals(0.0d, POSITIVE_ZERO.doubleValue(), 0);
        assertEquals(0.0f, POSITIVE_ZERO.floatValue(), 0);
        assertEquals(0L, POSITIVE_ZERO.longValue());
        assertEquals(0, POSITIVE_ZERO.intValue());

        assertEquals(NEGATIVE_ZERO.doubleValue(), -0d, 0);
        assertEquals(NEGATIVE_ZERO.floatValue(), -0f, 0);
        assertEquals(0L, NEGATIVE_ZERO.longValue());
        assertEquals(0, NEGATIVE_ZERO.intValue());

        assertEquals(parse("-0.0").doubleValue(), -0d, 0);
        assertEquals(parse("-0.0").floatValue(), -0f, 0);
        assertEquals(0L, parse("-0.0").longValue());
        assertEquals(0, parse("-0.0").intValue());

        assertEquals(5.4d, parse("5.4").doubleValue(), 0);
        assertEquals(5.4f, parse("5.4").floatValue(), 0);
        assertEquals(5L, parse("5.4").longValue());
        assertEquals(5, parse("5.4").intValue());

        assertEquals(1.2345678901234568E33d, parse("1234567890123456789012345678901234").doubleValue(), 0);
        assertEquals(1.2345679E33f, parse("1234567890123456789012345678901234").floatValue(), 0);
        assertEquals(Long.MAX_VALUE, parse("1234567890123456789012345678901234").longValue());
        assertEquals(Integer.MAX_VALUE, parse("1234567890123456789012345678901234").intValue());

        assertEquals(-1.2345678901234568E33d, parse("-1234567890123456789012345678901234").doubleValue(), 0);
        assertEquals(-1.2345679E33f, parse("-1234567890123456789012345678901234").floatValue(), 0);
        assertEquals(Long.MIN_VALUE, parse("-1234567890123456789012345678901234").longValue());
        assertEquals(Integer.MIN_VALUE, parse("-1234567890123456789012345678901234").intValue());
    }

    @Test
    public void shouldImplementComparable() {
        assertEquals(1, NaN.compareTo(NEGATIVE_ZERO));
        assertEquals(0, NaN.compareTo(NaN));
        assertEquals(1, NaN.compareTo(POSITIVE_INFINITY));
        assertEquals(1, NaN.compareTo(NEGATIVE_INFINITY));
        assertEquals(1, NaN.compareTo(parse("1")));
        assertEquals(1, POSITIVE_INFINITY.compareTo(NEGATIVE_INFINITY));
        assertEquals(0, POSITIVE_INFINITY.compareTo(POSITIVE_INFINITY));
        assertEquals(-1, POSITIVE_INFINITY.compareTo(NaN));
        assertEquals(1, POSITIVE_INFINITY.compareTo(NEGATIVE_ZERO));
        assertEquals(1, POSITIVE_INFINITY.compareTo(parse("1")));
        assertEquals(-1, NEGATIVE_INFINITY.compareTo(POSITIVE_INFINITY));
        assertEquals(0, NEGATIVE_INFINITY.compareTo(NEGATIVE_INFINITY));
        assertEquals(-1, NEGATIVE_INFINITY.compareTo(NaN));
        assertEquals(-1, NEGATIVE_INFINITY.compareTo(NEGATIVE_ZERO));
        assertEquals(-1, NEGATIVE_INFINITY.compareTo(parse("1")));
        assertEquals(-1, parse("1").compareTo(NaN));
        assertEquals(-1, parse("1").compareTo(POSITIVE_INFINITY));
        assertEquals(1, parse("1").compareTo(NEGATIVE_INFINITY));
        assertEquals(1, parse("1").compareTo(NEGATIVE_ZERO));
        assertEquals(-1, parse("-0").compareTo(parse("0")));
        assertEquals(0, parse("-0").compareTo(parse("-0")));
        assertEquals(-1, parse("-0").compareTo(NaN));
        assertEquals(-1, parse("-0").compareTo(POSITIVE_INFINITY));
        assertEquals(1, parse("-0").compareTo(NEGATIVE_INFINITY));
        assertEquals(1, parse("0").compareTo(parse("-0")));
        assertEquals(0, parse("0").compareTo(parse("0")));
        assertEquals(0, parse("5.4").compareTo(parse("5.4")));
        assertEquals(1, parse("5.4").compareTo(parse("5.3")));
        assertEquals(-1, parse("5.3").compareTo(parse("5.4")));
        assertEquals(0, parse("5.4").compareTo(parse("5.40")));
    }
}
