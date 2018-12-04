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

package org.bson.types

import spock.lang.Specification
import spock.lang.Unroll

import static org.bson.types.Decimal128.NEGATIVE_INFINITY
import static org.bson.types.Decimal128.NEGATIVE_NaN
import static org.bson.types.Decimal128.NEGATIVE_ZERO
import static org.bson.types.Decimal128.NaN
import static org.bson.types.Decimal128.POSITIVE_INFINITY
import static org.bson.types.Decimal128.POSITIVE_ZERO
import static org.bson.types.Decimal128.fromIEEE754BIDEncoding
import static org.bson.types.Decimal128.parse

class Decimal128Specification extends Specification {

    def 'should have correct constants'() {
        expect:
        POSITIVE_ZERO.equals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000000L))
        NEGATIVE_ZERO.equals(fromIEEE754BIDEncoding(0xb040000000000000L, 0x0000000000000000L))
        POSITIVE_INFINITY.equals(fromIEEE754BIDEncoding(0x7800000000000000L, 0x0000000000000000L))
        NEGATIVE_INFINITY.equals(fromIEEE754BIDEncoding(0xf800000000000000L, 0x0000000000000000L))
        NaN.equals(fromIEEE754BIDEncoding(0x7c00000000000000L, 0x0000000000000000L))
    }

    def 'should construct from high and low'() {
        given:
        def decimal = fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L)

        expect:
        decimal.high == 0x3040000000000000L
        decimal.low == 0x0000000000000001L
    }

    def 'should construct from simple string'() {
        expect:
        parse('0').equals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000000L))
        parse('-0').equals(fromIEEE754BIDEncoding(0xb040000000000000L, 0x0000000000000000L))
        parse('1').equals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L))
        parse('-1').equals(fromIEEE754BIDEncoding(0xb040000000000000L, 0x0000000000000001L))
        parse('12345678901234567').equals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x002bdc545d6b4b87L))
        parse('989898983458').equals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x000000e67a93c822L))
        parse('-12345678901234567').equals(fromIEEE754BIDEncoding(0xb040000000000000L, 0x002bdc545d6b4b87L))
        parse('0.12345').equals(fromIEEE754BIDEncoding(0x3036000000000000L, 0x0000000000003039L))
        parse('0.0012345').equals(fromIEEE754BIDEncoding(0x3032000000000000L, 0x0000000000003039L))
        parse('00012345678901234567').equals(fromIEEE754BIDEncoding(0x3040000000000000L, 0x002bdc545d6b4b87L))
    }

    def 'should round exactly'() {
        expect:
        parse('1.234567890123456789012345678901234').equals(parse('1.234567890123456789012345678901234'))
        parse('1.2345678901234567890123456789012340').equals(parse('1.234567890123456789012345678901234'))
        parse('1.23456789012345678901234567890123400').equals(parse('1.234567890123456789012345678901234'))
        parse('1.234567890123456789012345678901234000').equals(parse('1.234567890123456789012345678901234'))
    }

    def 'should clamp positive exponents'() {
        expect:
        parse('1E6112').equals(parse('10E6111'))
        parse('1E6113').equals(parse('100E6111'))
        parse('1E6143').equals(parse('100000000000000000000000000000000E+6111'))
        parse('1E6144').equals(parse('1000000000000000000000000000000000E+6111'))
        parse('11E6143').equals(parse('1100000000000000000000000000000000E+6111'))
        parse('0E8000').equals(parse('0E6111'))
        parse('0E2147483647').equals(parse('0E6111'))

        parse('-1E6112').equals(parse('-10E6111'))
        parse('-1E6113').equals(parse('-100E6111'))
        parse('-1E6143').equals(parse('-100000000000000000000000000000000E+6111'))
        parse('-1E6144').equals(parse('-1000000000000000000000000000000000E+6111'))
        parse('-11E6143').equals(parse('-1100000000000000000000000000000000E+6111'))
        parse('-0E8000').equals(parse('-0E6111'))
        parse('-0E2147483647').equals(parse('-0E6111'))
    }

    def 'should clamp negative exponents'() {
        expect:
        parse('0E-8000').equals(parse('0E-6176'))
        parse('0E-2147483647').equals(parse('0E-6176'))
        parse('10E-6177').equals(parse('1E-6176'))
        parse('100E-6178').equals(parse('1E-6176'))
        parse('110E-6177').equals(parse('11E-6176'))

        parse('-0E-8000').equals(parse('-0E-6176'))
        parse('-0E-2147483647').equals(parse('-0E-6176'))
        parse('-10E-6177').equals(parse('-1E-6176'))
        parse('-100E-6178').equals(parse('-1E-6176'))
        parse('-110E-6177').equals(parse('-11E-6176'))
    }

    def 'should construct from long'() {
        expect:
        new Decimal128(1L).equals(new Decimal128(new BigDecimal('1')))
        new Decimal128(Long.MIN_VALUE).equals(new Decimal128(new BigDecimal(Long.MIN_VALUE)))
        new Decimal128(Long.MAX_VALUE).equals(new Decimal128(new BigDecimal(Long.MAX_VALUE)))
    }

    def 'should construct from large BigDecimal'() {
        expect:
        parse('12345689012345789012345').equals(fromIEEE754BIDEncoding(0x304000000000029dL, 0x42da3a76f9e0d979L))
        parse('1234567890123456789012345678901234').equals(fromIEEE754BIDEncoding(0x30403cde6fff9732L, 0xde825cd07e96aff2L))
        parse('9.999999999999999999999999999999999E+6144').equals(fromIEEE754BIDEncoding(0x5fffed09bead87c0L, 0x378d8e63ffffffffL))
        parse('9.999999999999999999999999999999999E-6143').equals(fromIEEE754BIDEncoding(0x0001ed09bead87c0L, 0x378d8e63ffffffffL))
        parse('5.192296858534827628530496329220095E+33').equals(fromIEEE754BIDEncoding(0x3040ffffffffffffL, 0xffffffffffffffffL))
    }

    def 'should convert to simple BigDecimal'() {
        expect:
        fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000000L).bigDecimalValue().equals(new BigDecimal('0'))
        fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L).bigDecimalValue().equals(new BigDecimal('1'))
        fromIEEE754BIDEncoding(0xb040000000000000L, 0x0000000000000001L).bigDecimalValue().equals(new BigDecimal('-1'))
        fromIEEE754BIDEncoding(0x3040000000000000L, 0x002bdc545d6b4b87L).bigDecimalValue().equals(new BigDecimal('12345678901234567'))
        fromIEEE754BIDEncoding(0x3040000000000000L, 0x000000e67a93c822L).bigDecimalValue().equals(new BigDecimal('989898983458'))
        fromIEEE754BIDEncoding(0xb040000000000000L, 0x002bdc545d6b4b87L).bigDecimalValue().equals(new BigDecimal('-12345678901234567'))
        fromIEEE754BIDEncoding(0x3036000000000000L, 0x0000000000003039L).bigDecimalValue().equals(new BigDecimal('0.12345'))
        fromIEEE754BIDEncoding(0x3032000000000000L, 0x0000000000003039L).bigDecimalValue().equals(new BigDecimal('0.0012345'))
        fromIEEE754BIDEncoding(0x3040000000000000L, 0x002bdc545d6b4b87L).bigDecimalValue().equals(new BigDecimal('00012345678901234567'))
    }

    def 'should convert to large BigDecimal'() {
        expect:
        fromIEEE754BIDEncoding(0x304000000000029dL, 0x42da3a76f9e0d979L).bigDecimalValue() ==
                new BigDecimal('12345689012345789012345')

        fromIEEE754BIDEncoding(0x30403cde6fff9732L, 0xde825cd07e96aff2L).bigDecimalValue() ==
                new BigDecimal('1234567890123456789012345678901234')

        fromIEEE754BIDEncoding(0x5fffed09bead87c0L, 0x378d8e63ffffffffL).bigDecimalValue() ==
                new BigDecimal('9.999999999999999999999999999999999E+6144')

        fromIEEE754BIDEncoding(0x0001ed09bead87c0L, 0x378d8e63ffffffffL).bigDecimalValue() ==
                new BigDecimal('9.999999999999999999999999999999999E-6143')

        fromIEEE754BIDEncoding(0x3040ffffffffffffL, 0xffffffffffffffffL).bigDecimalValue() ==
                new BigDecimal('5.192296858534827628530496329220095E+33')
    }

    def 'should convert invalid representations of 0 as BigDecimal 0'() {
        expect:
        fromIEEE754BIDEncoding(0x6C10000000000000, 0x0).bigDecimalValue() == new BigDecimal('0')
        fromIEEE754BIDEncoding(0x6C11FFFFFFFFFFFF, 0xffffffffffffffffL).bigDecimalValue() == new BigDecimal('0E+3')
    }

    def 'should detect infinity'() {
        expect:
        POSITIVE_INFINITY.isInfinite()
        NEGATIVE_INFINITY.isInfinite()
        !parse('0').isInfinite()
        !parse('9.999999999999999999999999999999999E+6144').isInfinite()
        !parse('9.999999999999999999999999999999999E-6143').isInfinite()
        !POSITIVE_INFINITY.isFinite()
        !NEGATIVE_INFINITY.isFinite()
        parse('0').isFinite()
        parse('9.999999999999999999999999999999999E+6144').isFinite()
        parse('9.999999999999999999999999999999999E-6143').isFinite()
    }

    def 'should detect NaN'() {
        expect:
        NaN.isNaN()
        fromIEEE754BIDEncoding(0x7e00000000000000L, 0).isNaN()    // SNaN
        !POSITIVE_INFINITY.isNaN()
        !NEGATIVE_INFINITY.isNaN()
        !parse('0').isNaN()
        !parse('9.999999999999999999999999999999999E+6144').isNaN()
        !parse('9.999999999999999999999999999999999E-6143').isNaN()
    }

    def 'should convert NaN to string'() {
        expect:
        NaN.toString() == 'NaN'
    }

    def 'should convert NaN from string'() {
        expect:
        parse('NaN').equals(NaN)
        parse('nan').equals(NaN)
        parse('nAn').equals(NaN)
        parse('-NaN').equals(NEGATIVE_NaN)
        parse('-nan').equals(NEGATIVE_NaN)
        parse('-nAn').equals(NEGATIVE_NaN)
    }

    def 'should not convert NaN to BigDecimal'() {
        when:
        NaN.bigDecimalValue()

        then:
        thrown(ArithmeticException)
    }

    def 'should convert infinity to string'() {
        expect:
        POSITIVE_INFINITY.toString() == 'Infinity'
        NEGATIVE_INFINITY.toString() == '-Infinity'
    }

    def 'should convert infinity from string'() {
        expect:
        parse('Inf').equals(POSITIVE_INFINITY)
        parse('inf').equals(POSITIVE_INFINITY)
        parse('inF').equals(POSITIVE_INFINITY)
        parse('+Inf').equals(POSITIVE_INFINITY)
        parse('+inf').equals(POSITIVE_INFINITY)
        parse('+inF').equals(POSITIVE_INFINITY)
        parse('Infinity').equals(POSITIVE_INFINITY)
        parse('infinity').equals(POSITIVE_INFINITY)
        parse('infiniTy').equals(POSITIVE_INFINITY)
        parse('+Infinity').equals(POSITIVE_INFINITY)
        parse('+infinity').equals(POSITIVE_INFINITY)
        parse('+infiniTy').equals(POSITIVE_INFINITY)
        parse('-Inf').equals(NEGATIVE_INFINITY)
        parse('-inf').equals(NEGATIVE_INFINITY)
        parse('-inF').equals(NEGATIVE_INFINITY)
        parse('-Infinity').equals(NEGATIVE_INFINITY)
        parse('-infinity').equals(NEGATIVE_INFINITY)
        parse('-infiniTy').equals(NEGATIVE_INFINITY)
    }

    def 'should convert finite to string'() {
        expect:
        parse('0').toString() == '0'
        parse('-0').toString() == '-0'
        parse('0E10').toString() == '0E+10'
        parse('-0E10').toString() == '-0E+10'
        parse('1').toString() == '1'
        parse('-1').toString() == '-1'
        parse('-1.1').toString() == '-1.1'

        parse('123E-9').toString() == '1.23E-7'
        parse('123E-8').toString() == '0.00000123'
        parse('123E-7').toString() == '0.0000123'
        parse('123E-6').toString() == '0.000123'
        parse('123E-5').toString() == '0.00123'
        parse('123E-4').toString() == '0.0123'
        parse('123E-3').toString() == '0.123'
        parse('123E-2').toString() == '1.23'
        parse('123E-1').toString() == '12.3'
        parse('123E0').toString() == '123'
        parse('123E1').toString() == '1.23E+3'

        parse('1234E-7').toString() == '0.0001234'
        parse('1234E-6').toString() == '0.001234'

        parse('1E6').toString() == '1E+6'
    }

    def 'should convert invalid representations of 0 to string'() {
        expect:
        fromIEEE754BIDEncoding(0x6C10000000000000, 0x0).bigDecimalValue().toString() == '0'
        fromIEEE754BIDEncoding(0x6C11FFFFFFFFFFFF, 0xffffffffffffffffL).toString() == '0E+3'
    }


    def 'test equals'() {
        given:
        def d1 = fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L)
        def d2 = fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L)
        def d3 = fromIEEE754BIDEncoding(0x3040000000000001L, 0x0000000000000001L)
        def d4 = fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000011L)

        expect:
        d1.equals(d1)
        d1.equals(d2)
        !d1.equals(d3)
        !d1.equals(d4)
        !d1.equals(null)
        !d1.equals(0L)
    }

    def 'test hashCode'() {
        expect:
        fromIEEE754BIDEncoding(0x3040000000000000L, 0x0000000000000001L).hashCode() == 809500703
    }

    def 'should not convert infinity to BigDecimal'() {
        when:
        decimal.bigDecimalValue()

        then:
        thrown(ArithmeticException)

        where:
        decimal << [POSITIVE_INFINITY, NEGATIVE_INFINITY]
    }

    def 'should not convert negative zero to BigDecimal'() {
        when:
        decimal.bigDecimalValue()

        then:
        thrown(ArithmeticException)

        where:
        decimal << [parse('-0'), parse('-0E+1'), parse('-0E-1')]
    }

    def 'should not round inexactly'() {
        when:
        parse(val)

        then:
        thrown(IllegalArgumentException)

        where:
        val << [
                '12345678901234567890123456789012345E+6111',
                '123456789012345678901234567890123456E+6111',
                '1234567890123456789012345678901234567E+6111',
                '12345678901234567890123456789012345E-6176',
                '123456789012345678901234567890123456E-6176',
                '1234567890123456789012345678901234567E-6176',
                '-12345678901234567890123456789012345E+6111',
                '-123456789012345678901234567890123456E+6111',
                '-1234567890123456789012345678901234567E+6111',
                '-12345678901234567890123456789012345E-6176',
                '-123456789012345678901234567890123456E-6176',
                '-1234567890123456789012345678901234567E-6176',
        ]
    }

    def 'should not clamp large exponents if no extra precision is available'() {
        when:
        parse(val)

        then:
        thrown(IllegalArgumentException)

        where:
        val << [
                '1234567890123456789012345678901234E+6112',
                '1234567890123456789012345678901234E+6113',
                '1234567890123456789012345678901234E+6114',
                '-1234567890123456789012345678901234E+6112',
                '-1234567890123456789012345678901234E+6113',
                '-1234567890123456789012345678901234E+6114',
        ]
    }

    def 'should not clamp small exponents if no extra precision can be discarded'() {
        when:
        parse(val)

        then:
        thrown(IllegalArgumentException)

        where:
        val << [
                '1234567890123456789012345678901234E-6177',
                '1234567890123456789012345678901234E-6178',
                '1234567890123456789012345678901234E-6179',
                '-1234567890123456789012345678901234E-6177',
                '-1234567890123456789012345678901234E-6178',
                '-1234567890123456789012345678901234E-6179',
        ]
    }

    def 'should throw IllegalArgumentException if BigDecimal is too large'() {
        when:
        new Decimal128(new BigDecimal('12345678901234567890123456789012345'))

        then:
        thrown(IllegalArgumentException)
    }

    def 'should implement Number'() {
        expect:
        POSITIVE_INFINITY.doubleValue() == Double.POSITIVE_INFINITY
        POSITIVE_INFINITY.floatValue() == Float.POSITIVE_INFINITY
        POSITIVE_INFINITY.longValue() == Long.MAX_VALUE
        POSITIVE_INFINITY.intValue() == Integer.MAX_VALUE

        NEGATIVE_INFINITY.doubleValue() == Double.NEGATIVE_INFINITY
        NEGATIVE_INFINITY.floatValue() == Float.NEGATIVE_INFINITY
        NEGATIVE_INFINITY.longValue() == Long.MIN_VALUE
        NEGATIVE_INFINITY.intValue() == Integer.MIN_VALUE

        NaN.doubleValue() == Double.NaN
        NaN.floatValue() == Double.NaN
        NaN.longValue() == 0
        NaN.intValue() == 0

        NEGATIVE_NaN.doubleValue() == Double.NaN
        NEGATIVE_NaN.floatValue() == Float.NaN
        NEGATIVE_NaN.longValue() == 0
        NEGATIVE_NaN.intValue() == 0

        POSITIVE_ZERO.doubleValue() == 0.0d
        POSITIVE_ZERO.floatValue() == 0.0f
        POSITIVE_ZERO.longValue() == 0L
        POSITIVE_ZERO.intValue() == 0

        Double.compare(NEGATIVE_ZERO.doubleValue(), Double.valueOf('-0')) == 0
        Float.compare(NEGATIVE_ZERO.floatValue(), Float.valueOf('-0')) == 0
        NEGATIVE_ZERO.longValue() == 0L
        NEGATIVE_ZERO.intValue() == 0

        Double.compare(parse('-0.0').doubleValue(), Double.valueOf('-0')) == 0
        Float.compare(parse('-0.0').floatValue(), Float.valueOf('-0')) == 0
        parse('-0.0').longValue() == 0L
        parse('-0.0').intValue() == 0

        parse('5.4').doubleValue() == 5.4d
        parse('5.4').floatValue() == 5.4f
        parse('5.4').longValue() == 5L
        parse('5.4').intValue() == 5

        parse('1234567890123456789012345678901234').doubleValue() == 1.2345678901234568E33d
        parse('1234567890123456789012345678901234').floatValue() == 1.2345679E33f
        parse('1234567890123456789012345678901234').longValue() == Long.MAX_VALUE
        parse('1234567890123456789012345678901234').intValue() == Integer.MAX_VALUE

        parse('-1234567890123456789012345678901234').doubleValue() == -1.2345678901234568E33d
        parse('-1234567890123456789012345678901234').floatValue() == -1.2345679E33f
        parse('-1234567890123456789012345678901234').longValue() == Long.MIN_VALUE
        parse('-1234567890123456789012345678901234').intValue() == Integer.MIN_VALUE
    }

    @Unroll
    def '#first compareTo #second should equal #expectedValue'() {
        expect:
        first.compareTo(second) == expectedValue

        where:
        first             | second            | expectedValue
        NaN               | NEGATIVE_ZERO     | 1
        NaN               | NaN               | 0
        NaN               | POSITIVE_INFINITY | 1
        NaN               | NEGATIVE_INFINITY | 1
        NaN               | parse('1')        | 1
        POSITIVE_INFINITY | NEGATIVE_INFINITY | 1
        POSITIVE_INFINITY | POSITIVE_INFINITY | 0
        POSITIVE_INFINITY | NaN               | -1
        POSITIVE_INFINITY | NEGATIVE_ZERO     | 1
        POSITIVE_INFINITY | parse('1')        | 1
        NEGATIVE_INFINITY | POSITIVE_INFINITY | -1
        NEGATIVE_INFINITY | NEGATIVE_INFINITY | 0
        NEGATIVE_INFINITY | NaN               | -1
        NEGATIVE_INFINITY | NEGATIVE_ZERO     | -1
        NEGATIVE_INFINITY | parse('1')        | -1
        parse('1')        | NaN               | -1
        parse('1')        | POSITIVE_INFINITY | -1
        parse('1')        | NEGATIVE_INFINITY | 1
        parse('1')        | NEGATIVE_ZERO     | 1
        parse('-0')       | parse('0')        | -1
        parse('-0')       | parse('-0')       | 0
        parse('-0')       | NaN               | -1
        parse('-0')       | POSITIVE_INFINITY | -1
        parse('-0')       | NEGATIVE_INFINITY | 1
        parse('0')        | parse('-0')       | 1
        parse('0')        | parse('0')        | 0
        parse('5.4')      | parse('5.4')      | 0
        parse('5.4')      | parse('5.3')      | 1
        parse('5.3')      | parse('5.4')      | -1
        parse('5.4')      | parse('5.40')     | 0
    }
}
