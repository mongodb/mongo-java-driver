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
package com.mongodb.internal;

/**
 * A client side operation timeout factory that shares the generated {@link ClientSideOperationTimeout}.
 *
 * <p>Useful when operations that are combined and share timeouts e.g. Aggregation with a {@code $out} stage.</p>
 */
public final class ClientSideOperationTimeoutFactoryShared implements ClientSideOperationTimeoutFactory {
    private final ClientSideOperationTimeoutFactory factory;
    private ClientSideOperationTimeout wrapped;

    public ClientSideOperationTimeoutFactoryShared(final ClientSideOperationTimeoutFactory factory) {
        this.factory = factory;
    }

    @Override
    public synchronized ClientSideOperationTimeout create() {
        if (wrapped == null) {
             wrapped = factory.create();
        }
        return wrapped;
    }

    @Override
    public String toString() {
        return "ClientSideOperationTimeoutFactoryShared{"
                + "factory=" + factory
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        return factory.equals(o);
    }

    @Override
    public int hashCode() {
        return factory != null ? factory.hashCode() : 0;
    }
}
