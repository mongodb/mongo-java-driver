/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.mapping.lazy;

import com.google.code.morphia.Datastore;

/**
 * for use with DatastoreProvider.Default
 */
public final class DatastoreHolder {
    private static final DatastoreHolder INSTANCE = new DatastoreHolder();

    public static DatastoreHolder getInstance() {
        return INSTANCE;
    }

    private DatastoreHolder() {
    }

    private Datastore ds;

    public Datastore get() {
        return ds;
    }

    public void set(final Datastore ds) {
        this.ds = ds;
    }
}