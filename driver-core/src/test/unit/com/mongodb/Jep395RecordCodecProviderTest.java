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
package com.mongodb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class Jep395RecordCodecProviderTest {

    @Test
    @EnabledForJreRange(min = JRE.JAVA_17)
    void canSupportJavaRecordsOnJava17() {
        assertTrue(new Jep395RecordCodecProvider().hasRecordSupport());
    }

    @Test
    void doesNotErrorWhenCheckingNonRecords() {
        try {
            assertNull(new Jep395RecordCodecProvider().get(Integer.class, MongoClientSettings.getDefaultCodecRegistry()));
        } catch (Exception e) {
            fail("Should return null when checking for class");
        }
    }
}
