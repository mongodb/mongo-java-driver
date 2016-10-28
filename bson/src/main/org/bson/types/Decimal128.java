/*
 * Copyright 2016 MongoDB, Inc.
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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import static java.math.MathContext.DECIMAL128;
import static java.util.Arrays.asList;

/**
 * A binary integer decimal representation of a 128-bit decimal value, supporting 34 decimal digits of significand and an exponent range
 * of -6143 to +6144.
 *
 * @since 3.4
 * @see <a href="https://github.com/mongodb/specifications/blob/master/source/bson-decimal128/decimal128.rst">BSON Decimal128
 * specification</a>
 * @see <a href="https://en.wikipedia.org/wiki/Binary_Integer_Decimal">binary integer decimal</a>
 * @see <a href="https://en.wikipedia.org/wiki/Decimal128_floating-point_format">decimal128 floating-point format</a>
 * @see <a href="http://ieeexplore.ieee.org/document/4610935/">754-2008 - IEEE Standard for Floating-Point Arithmetic</a>
 */
public final class Decimal128 implements Serializable {

    private static final long serialVersionUID = 4570973266503637887L;

    private static final long INFINITY_MASK = 0x7800000000000000L;
    private static final long NaN_MASK = 0x7c00000000000000L;
    private static final long SIGN_BIT_MASK = 1L << 63;
    private static final int MIN_EXPONENT = -6176;
    private static final int MAX_EXPONENT = 6111;

    private static final int EXPONENT_OFFSET = 6176;
    private static final int MAX_BIT_LENGTH = 113;

    private static final BigInteger BIG_INT_TEN = new BigInteger("10");
    private static final BigInteger BIG_INT_ONE = new BigInteger("1");
    private static final BigInteger BIG_INT_ZERO = new BigInteger("0");

    private static final Set<String> NaN_STRINGS = new HashSet<String>(asList("nan"));
    private static final Set<String> NEGATIVE_NaN_STRINGS = new HashSet<String>(asList("-nan"));
    private static final Set<String> POSITIVE_INFINITY_STRINGS = new HashSet<String>(asList("inf", "+inf", "infinity", "+infinity"));
    private static final Set<String> NEGATIVE_INFINITY_STRINGS = new HashSet<String>(asList("-inf", "-infinity"));

    /**
     * A constant holding the positive infinity of type {@code Decimal128}.  It is equal to the value return by
     * {@code Decimal128.valueOf("Infinity")}.
     */
    public static final Decimal128 POSITIVE_INFINITY = fromIEEE754BIDEncoding(INFINITY_MASK, 0);

    /**
     * A constant holding the negative infinity of type {@code Decimal128}.  It is equal to the value return by
     * {@code Decimal128.valueOf("-Infinity")}.
     */
    public static final Decimal128 NEGATIVE_INFINITY = fromIEEE754BIDEncoding(INFINITY_MASK | SIGN_BIT_MASK, 0);

    /**
     * A constant holding a negative Not-a-Number (-NaN) value of type {@code Decimal128}.  It is equal to the value return by
     * {@code Decimal128.valueOf("-NaN")}.
     */
    public static final Decimal128 NEGATIVE_NaN = fromIEEE754BIDEncoding(NaN_MASK | SIGN_BIT_MASK, 0);

    /**
     * A constant holding a Not-a-Number (NaN) value of type {@code Decimal128}.  It is equal to the value return by
     * {@code Decimal128.valueOf("NaN")}.
     */
    public static final Decimal128 NaN = fromIEEE754BIDEncoding(NaN_MASK, 0);

    /**
     * A constant holding a postive zero value of type {@code Decimal128}.  It is equal to the value return by
     * {@code Decimal128.valueOf("0")}.
     */
    public static final Decimal128 POSITIVE_ZERO = fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000000L);

    /**
     * A constant holding a negative zero value of type {@code Decimal128}.  It is equal to the value return by
     * {@code Decimal128.valueOf("-0")}.
     */
    public static final Decimal128 NEGATIVE_ZERO = fromIEEE754BIDEncoding(0xb040000000000000L, 0x0000000000000000L);

    private final long high;
    private final long low;

    /**
     * Returns a Decimal128 value representing the given String.
     *
     * @param value the Decimal128 value represented as a String
     * @return the Decimal128 value representing the given String
     * @throws NumberFormatException if the value is out of the Decimal128 range
     * @see
     * <a href="https://github.com/mongodb/specifications/blob/master/source/bson-decimal128/decimal128.rst#from-string-representation">
     *     From-String Specification</a>
     */
    public static Decimal128 parse(final String value) {
        String lowerCasedValue = value.toLowerCase();

        if (NaN_STRINGS.contains(lowerCasedValue)) {
            return NaN;
        }
        if (NEGATIVE_NaN_STRINGS.contains(lowerCasedValue)) {
            return NEGATIVE_NaN;
        }
        if (POSITIVE_INFINITY_STRINGS.contains(lowerCasedValue)) {
            return POSITIVE_INFINITY;
        }
        if (NEGATIVE_INFINITY_STRINGS.contains(lowerCasedValue)) {
            return NEGATIVE_INFINITY;
        }
        return new Decimal128(new BigDecimal(value), value.charAt(0) == '-');
    }

    /**
     * Create an instance with the given high and low order bits representing this Decimal128 as an IEEE 754-2008 128-bit decimal
     * floating point using the BID encoding scheme.
     *
     * @param high the high-order 64 bits
     * @param low  the low-order 64 bits
     * @return the Decimal128 value representing the given high and low order bits
     */
    public static Decimal128 fromIEEE754BIDEncoding(final long high, final long low) {
        return new Decimal128(high, low);
    }

    /**
     * Constructs a Decimal128 value representing the given long.
     *
     * @param value the Decimal128 value represented as a long
     */
    public Decimal128(final long value) {
        this(new BigDecimal(value, DECIMAL128));
    }

    /**
     * Constructs a Decimal128 value representing the given BigDecimal.
     *
     * @param value the Decimal128 value represented as a BigDecimal
     * @throws NumberFormatException if the value is out of the Decimal128 range
     */
    public Decimal128(final BigDecimal value) {
        this(value, value.signum() == -1);
    }

    private Decimal128(final long high, final long low) {
        this.high = high;
        this.low = low;
    }

    // isNegative is necessary to detect -0, which can't be represented with a BigDecimal
    private Decimal128(final BigDecimal initialValue, final boolean isNegative) {
        long localHigh = 0;
        long localLow = 0;

        BigDecimal value = clampAndRound(initialValue);

        long exponent = -value.scale();

        if ((exponent < MIN_EXPONENT) || (exponent > MAX_EXPONENT)) {
            throw new AssertionError("Exponent is out of range for Decimal128 encoding: " + exponent); }

        if (value.unscaledValue().bitLength() > MAX_BIT_LENGTH) {
            throw new AssertionError("Unscaled roundedValue is out of range for Decimal128 encoding:" + value.unscaledValue());
        }

        BigInteger significand = value.unscaledValue().abs();
        int bitLength = significand.bitLength();

        for (int i = 0; i < Math.min(64, bitLength); i++) {
            if (significand.testBit(i)) {
                localLow |= 1L << i;
            }
        }

        for (int i = 64; i < bitLength; i++) {
            if (significand.testBit(i)) {
                localHigh |= 1L << (i - 64);
            }
        }

        long biasedExponent = exponent + EXPONENT_OFFSET;

        localHigh |= biasedExponent << 49;

        if (value.signum() == -1 || isNegative) {
            localHigh |= SIGN_BIT_MASK;
        }

        high = localHigh;
        low = localLow;
    }

    private BigDecimal clampAndRound(final BigDecimal initialValue) {
        BigDecimal value;
        if (-initialValue.scale() > MAX_EXPONENT) {
            int diff = -initialValue.scale() - MAX_EXPONENT;
            if (initialValue.unscaledValue().equals(BIG_INT_ZERO)) {
                value = new BigDecimal(initialValue.unscaledValue(), -MAX_EXPONENT);
            } else if (diff + initialValue.precision() > 34) {
                throw new NumberFormatException("Exponent is out of range for Decimal128 encoding of " + initialValue);
            } else {
                BigInteger multiplier = BIG_INT_TEN.pow(diff);
                value = new BigDecimal(initialValue.unscaledValue().multiply(multiplier), initialValue.scale() + diff);
            }
        } else if (-initialValue.scale() < MIN_EXPONENT) {
            // Increasing a very negative exponent may require decreasing precision, which is rounding
            // Only round exactly (by removing precision that is all zeroes).  An exception is thrown if the rounding would be inexact:
            // Exact:     .000...0011000  => 11000E-6177  => 1100E-6176  => .000001100
            // Inexact:   .000...0011001  => 11001E-6177  => 1100E-6176  => .000001100
            int diff = initialValue.scale() + MIN_EXPONENT;
            int undiscardedPrecision = ensureExactRounding(initialValue, diff);
            BigInteger divisor = undiscardedPrecision == 0 ? BIG_INT_ONE : BIG_INT_TEN.pow(diff);
            value = new BigDecimal(initialValue.unscaledValue().divide(divisor), initialValue.scale() - diff);
        } else {
            value = initialValue.round(DECIMAL128);
            int extraPrecision = initialValue.precision() - value.precision();
            if (extraPrecision > 0) {
                // Again, only round exactly
                ensureExactRounding(initialValue, extraPrecision);
            }
        }
        return value;
    }

    private int ensureExactRounding(final BigDecimal initialValue, final int extraPrecision) {
        String significand = initialValue.unscaledValue().abs().toString();
        int undiscardedPrecision = Math.max(0, significand.length() - extraPrecision);
        for (int i = undiscardedPrecision; i < significand.length(); i++) {
            if (significand.charAt(i) != '0') {
                throw new NumberFormatException("Conversion to Decimal128 would require inexact rounding of " + initialValue);
            }
        }
        return undiscardedPrecision;
    }

    /**
     * Gets the high-order 64 bits of the IEEE 754-2008 128-bit decimal floating point encoding for this Decimal128, using the BID encoding
     * scheme.
     *
     * @return the high-order 64 bits of this Decimal128
     */
    public long getHigh() {
        return high;
    }

    /**
     * Gets the low-order 64 bits of the IEEE 754-2008 128-bit decimal floating point encoding for this Decimal128, using the BID encoding
     * scheme.
     *
     * @return the low-order 64 bits of this Decimal128
     */
    public long getLow() {
        return low;
    }

    /**
     * Gets a BigDecimal that is equivalent to this Decimal128.
     *
     * @return a BigDecimal that is equivalent to this Decimal128
     * @throws ArithmeticException if the Decimal128 value is NaN, Infinity, -Infinity, or -0, none of which can be represented as a
     * BigDecimal
     */
    public BigDecimal bigDecimalValue() {

        if (isNaN()) {
            throw new ArithmeticException("NaN can not be converted to a BigDecimal");
        }

        if (isInfinite()) {
            throw new ArithmeticException("Infinity can not be converted to a BigDecimal");
        }

        BigDecimal bigDecimal = bigDecimalValueNoNegativeZeroCheck();

        // If the BigDecimal is 0, but the Decimal128 is negative, that means we have -0.
        if (isNegative() && bigDecimal.signum() == 0) {
            throw new ArithmeticException("Negative zero can not be converted to a BigDecimal");
        }

        return bigDecimal;
    }

    private BigDecimal bigDecimalValueNoNegativeZeroCheck() {
        int scale = -getExponent();

        if (twoHighestCombinationBitsAreSet()) {
            return BigDecimal.valueOf(0, scale);
        }

        return new BigDecimal(new BigInteger(isNegative() ? -1 : 1, getBytes()), scale);
    }

    // May have leading zeros.  Strip them before considering making this method public
    private byte[] getBytes() {
        byte[] bytes = new byte[15];

        long mask = 0x00000000000000ff;
        for (int i = 14; i >= 7; i--) {
            bytes[i] = (byte) ((low & mask) >>> ((14 - i) << 3));
            mask = mask << 8;
        }

        mask = 0x00000000000000ff;
        for (int i = 6; i >= 1; i--) {
            bytes[i] = (byte) ((high & mask) >>> ((6 - i) << 3));
            mask = mask << 8;
        }

        mask = 0x0001000000000000L;
        bytes[0] = (byte) ((high & mask) >>> 48);
        return bytes;
    }

    // Consider making this method public
    int getExponent() {
        if (twoHighestCombinationBitsAreSet()) {
            return (int) ((high & 0x1fffe00000000000L) >>> 47) - EXPONENT_OFFSET;
        } else {
            return (int) ((high & 0x7fff800000000000L) >>> 49) - EXPONENT_OFFSET;
        }
    }

    private boolean twoHighestCombinationBitsAreSet() {
        return (high & 3L << 61) == 3L << 61;
    }

    /**
     * Returns true if this Decimal128 is negative.
     *
     * @return true if this Decimal128 is negative
     */
    public boolean isNegative() {
        return (high & SIGN_BIT_MASK) == SIGN_BIT_MASK;
    }

    /**
     * Returns true if this Decimal128 is infinite.
     *
     * @return true if this Decimal128 is infinite
     */
    public boolean isInfinite() {
        return (high & INFINITY_MASK) == INFINITY_MASK;
    }

    /**
     * Returns true if this Decimal128 is finite.
     *
     * @return true if this Decimal128 is finite
     */
    public boolean isFinite() {
        return !isInfinite();
    }

    /**
     * Returns true if this Decimal128 is Not-A-Number (NaN).
     *
     * @return true if this Decimal128 is Not-A-Number
     */
    public boolean isNaN() {
        return (high & NaN_MASK) == NaN_MASK;
    }

    /**
     * Returns true if the encoded representation of this instance is the same as the encoded representation of {@code o}.
     * <p>
     * One consequence is that, whereas {@code Double.NaN != Double.NaN},
     * {@code new Decimal128("NaN").equals(new Decimal128("NaN")} returns true.
     * </p>
     * <p>
     * Another consequence is that, as with BigDecimal, {@code new Decimal128("1.0").equals(new Decimal128("1.00")} returns false,
     * because the precision is not the same and therefore the representation is not the same.
     * </p>
     *
     * @param o the object to compare for equality
     * @return true if the instances are equal
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Decimal128 that = (Decimal128) o;

        if (high != that.high) {
            return false;
        }
        if (low != that.low) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (low ^ (low >>> 32));
        result = 31 * result + (int) (high ^ (high >>> 32));
        return result;
    }

    /**
     * Returns the String representation of the Decimal128 value.
     *
     * @return the String representation
     * @see <a href="https://github.com/mongodb/specifications/blob/master/source/bson-decimal128/decimal128.rst#to-string-representation">
     *     To-String Sprecification</a>
     */
    @Override
    public String toString() {
        if (isNaN()) {
            return "NaN";
        }
        if (isInfinite()) {
            if (isNegative()) {
                return "-Infinity";
            } else {
                return "Infinity";
            }
        }
        return toStringWithBigDecimal();
    }

    private String toStringWithBigDecimal() {
        StringBuilder buffer = new StringBuilder();

        BigDecimal bigDecimal = bigDecimalValueNoNegativeZeroCheck();
        String significand = bigDecimal.unscaledValue().abs().toString();

        if (isNegative()) {
            buffer.append('-');
        }

        int exponent = -bigDecimal.scale();
        int adjustedExponent = exponent + (significand.length() - 1);
        if (exponent <= 0 && adjustedExponent >= -6) {
            if (exponent == 0) {
                buffer.append(significand);
            } else {
                int pad = -exponent - significand.length();
                if (pad >= 0) {
                    buffer.append('0');
                    buffer.append('.');
                    for (int i = 0; i < pad; i++) {
                        buffer.append('0');
                    }
                    buffer.append(significand, 0, significand.length());
                } else {
                    buffer.append(significand, 0, -pad);
                    buffer.append('.');
                    buffer.append(significand, -pad, -pad - exponent);
                }
            }
        } else {
            buffer.append(significand.charAt(0));
            if (significand.length() > 1) {
                buffer.append('.');
                buffer.append(significand, 1, significand.length());
            }
            buffer.append('E');
            if (adjustedExponent > 0) {
                buffer.append('+');
            }
            buffer.append(adjustedExponent);
        }
        return buffer.toString();
    }
}
