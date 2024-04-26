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

package com.mongodb.annotations;

/**
 * Enumerates the reasons an API element might be marked with annotations like {@link Alpha} or {@link Beta}.
 */
public enum Reason {
    /**
     * Indicates that the status of the driver API is the reason for the annotation.
     */
    CLIENT,

    /**
     * The driver API relies on the server API.
     * This dependency is the reason for the annotation and suggests that changes in the server API could impact the driver API.
     */
    SERVER
}
