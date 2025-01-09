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

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.bulk.ClientDeleteOneOptions;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ConcreteClientDeleteOneOptions extends AbstractClientDeleteOptions implements ClientDeleteOneOptions {
    static final ConcreteClientDeleteOneOptions MUTABLE_EMPTY = new ConcreteClientDeleteOneOptions();

    public ConcreteClientDeleteOneOptions() {
    }

    @Override
    public ConcreteClientDeleteOneOptions collation(@Nullable final Collation collation) {
        return (ConcreteClientDeleteOneOptions) super.collation(collation);
    }

    @Override
    public ConcreteClientDeleteOneOptions hint(@Nullable final Bson hint) {
        return (ConcreteClientDeleteOneOptions) super.hint(hint);
    }

    @Override
    public ConcreteClientDeleteOneOptions hintString(@Nullable final String hintString) {
        return (ConcreteClientDeleteOneOptions) super.hintString(hintString);
    }

    @Override
    public String toString() {
        return "ClientDeleteOneOptions{"
                + "collation=" + getCollation().orElse(null)
                + ", hint=" + getHint().orElse(null)
                + ", hintString=" + getHintString().map(s -> '\'' + s + '\'') .orElse(null)
                + '}';
    }
}
