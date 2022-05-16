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
 * Options for change stream pre- and post- images.
 *
 * @see CreateCollectionOptions
 * @since 4.7
 * @mongodb.driver.manual reference/command/create/ Create Collection
 */
public class ChangeStreamPreAndPostImagesOptions {
    private final boolean enabled;

    /**
     * Construct an instance
     *
     * @param enabled whether change stream pre- and post- images are enabled for the collection
     */
    public ChangeStreamPreAndPostImagesOptions(final boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Gets whether change stream pre- and post- images are enabled for the collection.
     *
     * @return whether change stream pre- and post- images are enabled for the collection
     */
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String toString() {
        return "ChangeStreamPreAndPostImagesOptions{"
                + "enabled=" + enabled
                + '}';
    }
}
