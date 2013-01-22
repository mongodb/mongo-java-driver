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

/**
 *
 */
package com.google.code.morphia.mapping.lazy;

import com.google.code.morphia.Datastore;

import java.io.Serializable;

/**
 * Lightweight object to be created (hopefully by a factoy some day) to create provide a Datastore-reference to a
 * resolving Proxy. If this was created by a common Object factory, it could make use of the current context (like Guice
 * Scopes etc.)
 *
 * @author uwe schaefer
 * @see LazyProxyFactory
 */
public interface DatastoreProvider extends Serializable {
    Datastore get();
}
