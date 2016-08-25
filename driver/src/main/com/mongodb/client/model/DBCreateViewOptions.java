/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.client.model;

/**
 * The options to apply when creating a view
 *
 * @since 3.4
 * @mongodb.server.release 3.4
 * @mongodb.driver.manual reference/command/create Create Command
 */
public class DBCreateViewOptions {
    private Collation collation;

    /**
     * Returns the collation options
     *
     * @return the collation options
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation
     *
     * @param collation the collation
     * @return this
     */
    public DBCreateViewOptions collation(final Collation collation) {
        this.collation = collation;
        return this;
    }
}
