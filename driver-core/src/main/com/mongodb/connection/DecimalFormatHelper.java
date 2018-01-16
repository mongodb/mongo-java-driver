/*
 * Copyright 2018 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.connection;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

final class DecimalFormatHelper {

    private static final DecimalFormatSymbols DECIMAL_FORMAT_SYMBOLS = initializeDecimalFormatSymbols();

    private DecimalFormatHelper(){
    }

    /**
     * Initialize the character used for decimal separator
     *
     * @return the character used for decimal separator
     * @since 3.6
     */
    private static DecimalFormatSymbols initializeDecimalFormatSymbols() {
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
        decimalFormatSymbols.setDecimalSeparator('.');
        return decimalFormatSymbols;
    }

    /**
     * Format a decimal number.
     *
     * @param pattern a non-localized pattern string
     * @param number the double number to format
     * @return the formatted String
     * @exception        ArithmeticException if rounding is needed with rounding
     *                   mode being set to RoundingMode.UNNECESSARY
     * @see java.text.DecimalFormat
     * @see java.text.NumberFormat#format
     *
     * @since 3.7
     */
    public static String format(final String pattern, final double number){
        return new DecimalFormat(pattern, DECIMAL_FORMAT_SYMBOLS).format(number);
    }

}
