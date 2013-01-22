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

package com.google.code.morphia.mapping;

import com.google.code.morphia.ObjectFactory;

/**
 * Options to control mapping behavior.
 *
 * @author Scott Hernandez
 */
public class MapperOptions {
    /**
     * <p>Treat java transient fields as if they have <code>@Transient</code> on them</p>
     */
    public final boolean actLikeSerializer = false;
    /**
     * <p>Controls if null are stored. </p>
     */
    public final boolean storeNulls = false;
    /**
     * <p>Controls if empty collection/arrays are stored. </p>
     */
    public boolean storeEmpties = false;
    /**
     * <p>Controls if final fields are stored. </p>
     */
    public boolean ignoreFinals = false; //ignore final fields.

    public final CustomMapper referenceMapper = new ReferenceMapper();
    public final CustomMapper embeddedMapper = new EmbeddedMapper();
    public final CustomMapper valueMapper = new ValueMapper();
    public final CustomMapper defaultMapper = embeddedMapper;

    public final ObjectFactory objectFactory = new DefaultCreator();
}
