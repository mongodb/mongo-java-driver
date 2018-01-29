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

package com.mongodb.client.model;

/**
 * The options to apply when renaming a collection.
 *
 * @mongodb.driver.manual reference/command/renameCollection renameCollection
 * @since 3.0
 */
public class RenameCollectionOptions {
    private boolean dropTarget;

    /**
     * Gets if mongod should drop the target of renameCollection prior to renaming the collection.
     *
     * @return true if mongod should drop the target of renameCollection prior to renaming the collection.
     */
    public boolean isDropTarget() {
        return dropTarget;
    }

    /**
     * Sets if mongod should drop the target of renameCollection prior to renaming the collection.
     *
     * @param dropTarget true if mongod should drop the target of renameCollection prior to renaming the collection.
     * @return this
     */
    public RenameCollectionOptions dropTarget(final boolean dropTarget) {
        this.dropTarget = dropTarget;
        return this;
    }

}
