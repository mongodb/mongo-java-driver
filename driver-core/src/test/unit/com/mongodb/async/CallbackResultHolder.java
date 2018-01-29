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

package com.mongodb.async;

/**
 * A SingleResultCallback implementation that saves the result of the callback.
 *
 * @param <T> the result type
 * @since 3.0
 */
class CallbackResultHolder<T> implements SingleResultCallback<T> {
    private T result = null;
    private Throwable error = null;
    private boolean isDone = false;

    /**
     * Set the result of the callback
     *
     * @param result the result of the callback
     * @param error  the throwable error of the callback
     */
    public void onResult(final T result, final Throwable error) {
        if (isDone) {
            throw new IllegalStateException("The CallbackResult cannot be initialized multiple times.");
        }
        this.result = result;
        this.error = error;
        this.isDone = true;
    }

    /**
     * Returns the result of the callback or null.
     *
     * @return the result of the callback if completed or null
     */
    public T getResult() {
        return result;
    }

    /**
     * Gets the error result of the callback or null.
     *
     * @return the error result of the callback or null
     */
    public Throwable getError() {
        return error;
    }

    /**
     * Returns true if the callback returned an error.
     *
     * @return true if the callback returned an error
     */
    public boolean hasError() {
        return error != null;
    }

    /**
     * Returns true if the callback has been called.
     *
     * @return true if the callback has been called
     */
    public boolean isDone() {
        return isDone;
    }

    @Override
    public String toString() {
        return "CallbackResultHolder{"
               + "result=" + result
               + ", error=" + error
               + ", isDone=" + isDone
               + '}';
    }
}
