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

package com.mongodb.internal.async;

import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time.</p>
 */
public class ErrorHandlingResultCallback<T> implements SingleResultCallback<T> {
    private final SingleResultCallback<T> wrapped;
    private final Logger logger;

    public static <T> SingleResultCallback<T> errorHandlingCallback(final SingleResultCallback<T> callback, final Logger logger) {
        if (callback instanceof ErrorHandlingResultCallback) {
            return callback;
        } else {
            return new ErrorHandlingResultCallback<>(callback, logger);
        }
    }

    ErrorHandlingResultCallback(final SingleResultCallback<T> wrapped, final Logger logger) {
        this.wrapped = notNull("wrapped", wrapped);
        this.logger = notNull("logger", logger);
    }

    @Override
    public void onResult(@Nullable final T result, @Nullable final Throwable t) {
        try {
            wrapped.onResult(result, t);
        } catch (Throwable e) {
            logger.error("Callback onResult call produced an error", e);
        }
    }

}
