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
package com.mongodb.client.cursor;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;

import java.util.concurrent.TimeUnit;

/**
 * The timeout mode for a cursor
 *
 * <p>For operations that create cursors, {@code timeoutMS} can either cap the lifetime of the cursor or be applied separately to the
 * original operation and all next calls.
 * </p>
 * @see com.mongodb.MongoClientSettings#getTimeout(TimeUnit)
 * @since CSOT
 */
@Alpha(Reason.CLIENT)
public enum TimeoutMode {

    /**
     * The timeout lasts for the lifetime of the cursor
     */
    CURSOR_LIFETIME,

    /**
     * The timeout is reset for each batch iteration of the cursor
     */
    ITERATION
}
