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

package com.mongodb.internal.session;

import com.mongodb.ClientSessionOptions;
import com.mongodb.session.ClientSession;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BaseClientSessionImplTest {

    @Test
    void shouldNotCheckoutServerSessionIfNeverRequested() {
        ServerSessionPool serverSessionPool = new ServerSessionPool(getCluster(), OPERATION_CONTEXT);
        ClientSession clientSession = new BaseClientSessionImpl(serverSessionPool, new Object(), ClientSessionOptions.builder().build());

        assertEquals(0, serverSessionPool.getInUseCount());

        clientSession.close();

        assertEquals(0, serverSessionPool.getInUseCount());
    }

    @Test
    void shouldDelayServerSessionCheckoutUntilRequested() {
        ServerSessionPool serverSessionPool = new ServerSessionPool(getCluster(), OPERATION_CONTEXT);
        ClientSession clientSession = new BaseClientSessionImpl(serverSessionPool, new Object(), ClientSessionOptions.builder().build());

        assertEquals(0, serverSessionPool.getInUseCount());

        clientSession.getServerSession();

        assertEquals(1, serverSessionPool.getInUseCount());

        clientSession.close();

        assertEquals(0, serverSessionPool.getInUseCount());
    }
}
