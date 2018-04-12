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

package com.mongodb.internal.authentication;

import org.junit.Test;

import static com.mongodb.internal.authentication.SaslPrep.saslPrepQuery;
import static com.mongodb.internal.authentication.SaslPrep.saslPrepStored;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SaslPrepTest {

    @Test
    public void rfc4013Examples() {
        // Taken from https://tools.ietf.org/html/rfc4013#section-3
        assertEquals("IX", saslPrepStored("I\u00ADX"));
        assertEquals("user", saslPrepStored("user"));
        assertEquals("user=", saslPrepStored("user="));
        assertEquals("USER", saslPrepStored("USER"));
        assertEquals("a", saslPrepStored("\u00AA"));
        assertEquals("IX", saslPrepStored("\u2168"));
        try {
            saslPrepStored("\u0007");
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Prohibited character at position 0", e.getMessage());
        }
        try {
            saslPrepStored("\u0627\u0031");
            fail("Should thow IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("First character is RandALCat, but last character is not", e.getMessage());
        }
    }

    @Test
    public void mappedToSpace() {
        assertEquals("A B", saslPrepStored("A\u00A0B"));
    }

    @Test
    public void bidi2() {
        // RandALCat character first *and* last is OK
        assertEquals("\u0627\u0031\u0627", saslPrepStored("\u0627\u0031\u0627"));
        // Both RandALCat character and LCat is not allowed
        try {
            saslPrepStored("\u0627\u0041\u0627");
            fail("Should thow IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("Contains both RandALCat characters and LCat characters", e.getMessage());
        }
    }

    @Test
    public void unassigned() {
        int unassignedCodepoint;
        for (unassignedCodepoint = Character.MAX_CODE_POINT;
             unassignedCodepoint >= Character.MIN_CODE_POINT;
             unassignedCodepoint--) {
            if (!Character.isDefined(unassignedCodepoint)
                    && !SaslPrep.prohibited(unassignedCodepoint)) {
                break;
            }
        }
        String withUnassignedChar = "abc" + new String(Character.toChars(unassignedCodepoint));
        assertEquals(withUnassignedChar, saslPrepQuery(withUnassignedChar));
        try {
            saslPrepStored(withUnassignedChar);
        } catch (IllegalArgumentException e) {
            assertEquals("Character at position 3 is unassigned", e.getMessage());
        }
    }
}
