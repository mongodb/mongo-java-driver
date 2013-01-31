/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.utils;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PrePersist;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;

public abstract class LongIdEntity {
    @Id
    protected Long myLongId;

    @Transient
    protected final Datastore ds;

    protected LongIdEntity(final Datastore ds) {
        this.ds = ds;
    }

    @PrePersist
    void prePersist() {
        if (myLongId == null) {
            final String collName = ds.getCollection(getClass()).getName();
            final Query<StoredId> q = ds.find(StoredId.class, "_id", collName);
            final UpdateOperations<StoredId> uOps = ds.createUpdateOperations(StoredId.class).inc("value");
            StoredId newId = ds.findAndModify(q, uOps);
            if (newId == null) {
                newId = new StoredId(collName);
                ds.save(newId);
            }

            myLongId = newId.getValue();
        }
    }

    /**
     * Used to store counters for other entities.
     *
     * @author skot
     */

    @Entity(value = "ids", noClassnameStored = true)
    public static class StoredId {
        @Id
        final String className;
        protected final Long value = 1L;

        public StoredId(final String name) {
            className = name;
        }

        protected StoredId() {
            className = "";
        }

        public Long getValue() {
            return value;
        }
    }
}
