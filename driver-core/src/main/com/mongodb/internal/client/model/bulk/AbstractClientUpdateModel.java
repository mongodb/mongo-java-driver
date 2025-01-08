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
package com.mongodb.internal.client.model.bulk;

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.Optional;

import static com.mongodb.assertions.Assertions.assertTrue;
import static java.util.Optional.ofNullable;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public abstract class AbstractClientUpdateModel<O extends AbstractClientUpdateOptions> {
    private final Bson filter;
    @Nullable
    private final Bson update;
    @Nullable
    private final Iterable<? extends Bson> updatePipeline;
    private final O options;

    AbstractClientUpdateModel(
            final Bson filter,
            @Nullable
            final Bson update,
            @Nullable final Iterable<? extends Bson> updatePipeline,
            final O options) {
        this.filter = filter;
        assertTrue(update == null ^ updatePipeline == null);
        this.update = update;
        this.updatePipeline = updatePipeline;
        this.options = options;
    }

    public final Bson getFilter() {
        return filter;
    }

    public final Optional<Bson> getUpdate() {
        return ofNullable(update);
    }

    public final Optional<Iterable<? extends Bson>> getUpdatePipeline() {
        return ofNullable(updatePipeline);
    }

    public final O getOptions() {
        return options;
    }

    abstract String getToStringDescription();

    @Override
    public final String toString() {
        return getToStringDescription()
                + "{filter=" + filter
                + ", update=" + (update != null ? update : updatePipeline)
                + ", options=" + options
                + '}';
    }
}
