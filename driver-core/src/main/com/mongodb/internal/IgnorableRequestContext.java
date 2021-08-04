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

import com.mongodb.RequestContext;

import java.util.Map;
import java.util.stream.Stream;

/**
 * A {@code RequestContext} that can be ignored by the driver.  Useful to ensure that we always
 * have a non-null {@code RequestContext} to pass around the driver.
 */
public final class IgnorableRequestContext implements RequestContext {

    public static final IgnorableRequestContext INSTANCE = new IgnorableRequestContext();

    private IgnorableRequestContext() {
    }

    @Override
    public <T> T get(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasKey(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(final Object key, final Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<Map.Entry<Object, Object>> stream() {
        throw new UnsupportedOperationException();
    }
}
