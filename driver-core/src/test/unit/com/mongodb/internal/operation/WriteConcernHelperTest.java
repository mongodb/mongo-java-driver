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

package com.mongodb.internal.operation;

import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.WriteConcernError;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteConcernHelperTest {

    static WriteConcern[] shouldRemoveWtimeout(){
        return new WriteConcern[]{
                WriteConcern.ACKNOWLEDGED,
                WriteConcern.MAJORITY,
                WriteConcern.W1,
                WriteConcern.W2,
                WriteConcern.W3,
                WriteConcern.UNACKNOWLEDGED,
                WriteConcern.JOURNALED,

                WriteConcern.ACKNOWLEDGED.withWTimeout(100, TimeUnit.MILLISECONDS),
                WriteConcern.MAJORITY.withWTimeout(100, TimeUnit.MILLISECONDS),
                WriteConcern.W1.withWTimeout(100, TimeUnit.MILLISECONDS),
                WriteConcern.W2.withWTimeout(100, TimeUnit.MILLISECONDS),
                WriteConcern.W3.withWTimeout(100, TimeUnit.MILLISECONDS),
                WriteConcern.UNACKNOWLEDGED.withWTimeout(100, TimeUnit.MILLISECONDS),
                WriteConcern.JOURNALED.withWTimeout(100, TimeUnit.MILLISECONDS),
        };
    }

    @MethodSource
    @ParameterizedTest
    void shouldRemoveWtimeout(final WriteConcern writeConcern){
        //when
        WriteConcern clonedWithoutTimeout = WriteConcernHelper.cloneWithoutTimeout(writeConcern);

        //then
        assertEquals(writeConcern.getWObject(), clonedWithoutTimeout.getWObject());
        assertEquals(writeConcern.getJournal(), clonedWithoutTimeout.getJournal());
        assertNull(clonedWithoutTimeout.getWTimeout(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldCreateWriteConcernErrorWhenErrmsgIsMissing() {
        //given
        BsonDocument writeConcernErrorDocument = BsonDocument.parse("{code: 79, codeName: 'UnknownReplWriteConcern'}");

        //when
        WriteConcernError writeConcernError = WriteConcernHelper.createWriteConcernError(writeConcernErrorDocument);

        //then
        assertEquals(79, writeConcernError.getCode());
        assertEquals("UnknownReplWriteConcern", writeConcernError.getCodeName());
        assertEquals("", writeConcernError.getMessage());
        assertTrue(writeConcernError.getDetails().isEmpty());
    }

    @Test
    void shouldCreateWriteConcernExceptionWhenErrmsgIsMissing() {
        //given
        BsonDocument result = BsonDocument.parse("{ok: 1, n: 1, writeConcernError: {code: 79, codeName: 'UnknownReplWriteConcern'}}");

        //when
        MongoWriteConcernException exception = WriteConcernHelper.createWriteConcernException(result, new ServerAddress());

        //then
        assertEquals(79, exception.getCode());
        assertEquals(79, exception.getWriteConcernError().getCode());
        assertEquals("", exception.getWriteConcernError().getMessage());
    }
}
