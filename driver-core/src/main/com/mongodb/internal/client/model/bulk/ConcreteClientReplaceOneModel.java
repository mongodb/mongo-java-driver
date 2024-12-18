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

import com.mongodb.client.model.bulk.ClientReplaceOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientReplaceOneModel implements ClientWriteModel {
    private final Bson filter;
    private final Object replacement;
    private final ConcreteClientReplaceOptions options;

    public ConcreteClientReplaceOneModel(final Bson filter, final Object replacement, @Nullable final ClientReplaceOptions options) {
        this.filter = filter;
        this.replacement = replacement;
        this.options = options == null ? ConcreteClientReplaceOptions.MUTABLE_EMPTY : (ConcreteClientReplaceOptions) options;
    }

    public Bson getFilter() {
        return filter;
    }

    public Object getReplacement() {
        return replacement;
    }

    public ConcreteClientReplaceOptions getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return "ClientReplaceOneModel{"
                + "filter=" + filter
                + ", replacement=" + replacement
                + ", options=" + options
                + '}';
    }
}
