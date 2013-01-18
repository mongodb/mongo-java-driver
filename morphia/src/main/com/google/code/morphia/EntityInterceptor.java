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

package com.google.code.morphia;

import com.google.code.morphia.annotations.PostPersist;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.annotations.PrePersist;
import com.google.code.morphia.annotations.PreSave;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;

/**
 * Interface for intercepting @Entity lifecycle events
 */
public interface EntityInterceptor {
    /**
     * see {@link PrePersist}
     */
    void prePersist(Object ent, DBObject dbObj, Mapper mapr);

    /**
     * see {@link PreSave}
     */
    void preSave(Object ent, DBObject dbObj, Mapper mapr);

    /**
     * see {@link PostPersist}
     */
    void postPersist(Object ent, DBObject dbObj, Mapper mapr);

    /**
     * see {@link PreLoad}
     */
    void preLoad(Object ent, DBObject dbObj, Mapper mapr);

    /**
     * see {@link postLoad}
     */
    void postLoad(Object ent, DBObject dbObj, Mapper mapr);
}
