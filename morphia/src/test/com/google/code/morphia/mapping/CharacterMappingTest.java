/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
package com.google.code.morphia.mapping;

import com.google.code.morphia.Key;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class CharacterMappingTest extends TestBase {
    public static class ContainsChar {
        @Id
        private ObjectId id;
        private char c;
    }

    public static class ContainsCharacter {
        @Id
        private ObjectId id;
        private Character c;
    }

    public static class ContainsCharArray {
        @Id
        private ObjectId id;
        private char[] c;
    }

    public static class ContainsCharacterArray {
        @Id
        private ObjectId id;
        private Character[] c;
    }


    @Test
    public void testCharMapping() throws Exception {
        morphia.map(ContainsChar.class);
        final ContainsChar entity = new ContainsChar();
        final char testChar = 'a';
        entity.c = testChar;
        final Key<ContainsChar> savedKey = ds.save(entity);
        final ContainsChar loaded = ds.get(ContainsChar.class, savedKey.getId());
        Assert.assertEquals(testChar, loaded.c);
        Assert.assertNotNull(loaded.id);
    }

    @Test
    public void testCharacterMapping() throws Exception {
        morphia.map(ContainsCharacter.class);
        final ContainsCharacter entity = new ContainsCharacter();
        final Character testChar = 'a';
        entity.c = testChar;
        final Key<ContainsCharacter> savedKey = ds.save(entity);
        final ContainsCharacter loaded = ds.get(ContainsCharacter.class, savedKey.getId());
        Assert.assertEquals(testChar, loaded.c);
        Assert.assertNotNull(loaded.id);
    }

    @Test
    public void testCharArrayMapping() throws Exception {
        morphia.map(ContainsCharArray.class);
        ContainsCharArray entity = new ContainsCharArray();
        final char[] testChar = "My Hoovercraft is full of eels".toCharArray();
        entity.c = testChar;
        ds.save(entity);
        entity = ds.get(entity);

        for (int i = 0; i < testChar.length; i++) {
            final char c = testChar[i];
            Assert.assertEquals(c, entity.c[i]);
        }
        Assert.assertNotNull(entity.id);
    }

    @Test
    public void testCharacterArrayMapping() throws Exception {
        morphia.map(ContainsCharacterArray.class);
        ContainsCharacterArray entity = new ContainsCharacterArray();
        final Character[] testChar = new Character[]{'a', 'b'};
        entity.c = testChar;
        ds.save(entity);
        entity = ds.get(entity);

        Assert.assertEquals(testChar[0], entity.c[0]);
        Assert.assertEquals(testChar[1], entity.c[1]);
        Assert.assertNotNull(entity.id);
    }

}
