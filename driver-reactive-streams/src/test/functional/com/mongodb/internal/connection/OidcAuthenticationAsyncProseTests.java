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

package com.mongodb.internal.connection;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.junit.jupiter.api.Test;
import reactivestreams.helpers.SubscriberHelpers;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static util.ThreadTestHelpers.executeAll;

public class OidcAuthenticationAsyncProseTests extends OidcAuthenticationProseTests {

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return new SyncMongoClient(MongoClients.create(settings));
    }

    @Test
    public void testNonblockingCallbacks() {
        // not a prose spec test
        delayNextFind();

        int simulatedDelayMs = 100;
        TestCallback requestCallback = createCallback().setExpired().setDelayMs(simulatedDelayMs);
        TestCallback refreshCallback = createCallback().setDelayMs(simulatedDelayMs);

        MongoClientSettings clientSettings = createSettings(OIDC_URL, requestCallback, refreshCallback);

        try (com.mongodb.reactivestreams.client.MongoClient client = MongoClients.create(clientSettings)) {
            executeAll(2, () -> {
                SubscriberHelpers.OperationSubscriber<Object> subscriber = new SubscriberHelpers.OperationSubscriber<>();
                long t1 = System.nanoTime();
                client.getDatabase("test")
                        .getCollection("test")
                        .find()
                        .first()
                        .subscribe(subscriber);
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);

                assertTrue(elapsedMs < simulatedDelayMs);
                subscriber.get();
            });

            // ensure both callbacks have been tested
            assertEquals(1, requestCallback.getInvocations());
            assertEquals(1, refreshCallback.getInvocations());
        }
    }
}
