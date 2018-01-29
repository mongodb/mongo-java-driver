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

package com.mongodb.connection;

/**
 * Listener for ChangeEvents - classes that implement this will be informed if classes of type {@code T} are changed.
 *
 * @param <T> the type of the value that changed.
 */
interface ChangeListener<T> {
    /**
     * A {@code ChangeEvent} has been fired to notify listeners that {@code T} has changed.
     *
     * @param event an event containing the old and new values of {@code T}.
     */
    void stateChanged(ChangeEvent<T> event);
}
