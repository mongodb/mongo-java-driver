/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

/**
 *
 */
package com.google.code.morphia.converters;

import com.google.code.morphia.TestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class LocaleConverterTest extends TestBase {
    @Test
    public void testConv() throws Exception {
        final LocaleConverter c = new LocaleConverter();

        Locale l = Locale.CANADA_FRENCH;
        Locale l2 = (Locale) c.decode(Locale.class, c.encode(l));
        Assert.assertEquals(l, l2);

        l = new Locale("de", "DE", "bavarian");
        l2 = (Locale) c.decode(Locale.class, c.encode(l));
        Assert.assertEquals(l, l2);
        Assert.assertEquals("de", l2.getLanguage());
        Assert.assertEquals("DE", l2.getCountry());
        Assert.assertEquals("bavarian", l2.getVariant());

    }
}
