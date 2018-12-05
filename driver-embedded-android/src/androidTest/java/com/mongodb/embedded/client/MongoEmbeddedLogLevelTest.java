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

package com.mongodb.embedded.client;

import com.mongodb.embedded.capi.LogLevel;
import org.junit.Test;
import org.junit.runner.RunWith;
import android.support.test.runner.AndroidJUnit4;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(AndroidJUnit4.class)
public class MongoEmbeddedLogLevelTest {

    @Test
    public void shouldMirrorCAPILogLevel() {
        assertEquals(LogLevel.LOGGER, MongoEmbeddedLogLevel.LOGGER.toCapiLogLevel());
        assertEquals(LogLevel.NONE, MongoEmbeddedLogLevel.NONE.toCapiLogLevel());
        assertEquals(LogLevel.STDERR, MongoEmbeddedLogLevel.STDERR.toCapiLogLevel());
        assertEquals(LogLevel.STDOUT, MongoEmbeddedLogLevel.STDOUT.toCapiLogLevel());
    }

    @Test
    public void shouldHaveTheSameNamedEnumConstantsAsLogLevel() {

        List<String> mongoEmbeddedLogLevels = new ArrayList<String>();
        for (MongoEmbeddedLogLevel enumConstant : MongoEmbeddedLogLevel.class.getEnumConstants()) {
            mongoEmbeddedLogLevels.add(enumConstant.name());
        }

        List<String> capiLogLevels = new ArrayList<String>();
        for (LogLevel enumConstant : LogLevel.class.getEnumConstants()) {
            capiLogLevels.add(enumConstant.name());
        }

        assertEquals(new HashSet<String>(capiLogLevels), new HashSet<String>(mongoEmbeddedLogLevels));
    }
}
