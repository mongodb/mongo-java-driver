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

package org.mongodb.operation;

import org.junit.Test;
import org.mongodb.ReadPreference;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerType;

import java.net.UnknownHostException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mongodb.connection.ClusterDescription.Mode.Discovering;

public class ReadPreferenceServerSelectorTest {
    @Test
    public void testAll() throws UnknownHostException {
        ReadPreferenceServerSelector selector = new ReadPreferenceServerSelector(ReadPreference.primary());

        assertEquals(ReadPreference.primary(), selector.getReadPreference());

        assertEquals(new ReadPreferenceServerSelector(ReadPreference.primary()), selector);
        assertNotEquals(new ReadPreferenceServerSelector(ReadPreference.secondary()), selector);
        assertNotEquals(new Object(), selector);

        assertEquals(new ReadPreferenceServerSelector(ReadPreference.primary()).hashCode(), selector.hashCode());

        assertEquals("ReadPreferenceServerSelector{readPreference=primary}", selector.toString());

        final ServerDescription primary = ServerDescription.builder()
                .address(new ServerAddress())
                .ok(true)
                .type(ServerType.ReplicaSetPrimary)
                .build();
        assertEquals(Arrays.asList(primary), selector.choose(new ClusterDescription(Arrays.asList(primary), 15, Discovering)));
    }
}
