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

package org.mongodb.connection;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class ServerDescriptionTest {

    @Test
    public void testDefaults() {
        ServerDescription serverDescription = ServerDescription.builder().build();
        assertFalse(serverDescription.isOk());
        assertFalse(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());
        assertEquals(0F, serverDescription.getElapsedMillis(), 0F);
        assertEquals(0x1000000, serverDescription.getMaxDocumentSize());
        assertEquals(0x2000000, serverDescription.getMaxMessageSize());
        assertNull(serverDescription.getPrimary());
        assertEquals(Collections.<String>emptyList(), serverDescription.getHosts());
        assertEquals(Collections.<Tag>emptySet(), serverDescription.getTags());
        assertEquals(Collections.<String>emptyList(), serverDescription.getHosts());
        assertEquals(Collections.<String>emptyList(), serverDescription.getPassives());
        assertNull(serverDescription.getSetName());
    }
}
