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

package com.mongodb.client.model.changestream;

/**
 * The level at which the change stream operation operates at.
 *
 * @since 3.8
 * @mongodb.server.release 4.0
 */
public enum ChangeStreamLevel {
    /**
     * Observing all changes on the Client
     */
    CLIENT,

    /**
     * Observing all changes on a specific database
     */
    DATABASE,

    /**
     * Observing all changes on a specific collection
     */
    COLLECTION
}
