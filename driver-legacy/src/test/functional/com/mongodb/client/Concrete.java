/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.client;

import org.bson.types.ObjectId;

class Concrete {
    private ObjectId id;
    private final String str;
    private final int i;
    private final long l;
    private final double d;
    private final long date;

    Concrete(final String str, final int i, final long l, final double d, final long date) {
        this.str = str;
        this.i = i;
        this.l = l;
        this.d = d;
        this.date = date;
    }

    Concrete(final ObjectId id, final String str, final int i, final long l, final double d, final long date) {
        this(str, i, l, d, date);
        this.id = id;
    }

    @Override
    public String toString() {
        return "Concrete{"
               + "id=" + id
               + ", str='" + str + '\''
               + ", i=" + i
               + ", l=" + l
               + ", d=" + d
               + ", date=" + date
               + '}';
    }

    ObjectId getId() {
        return id;
    }

    String getStr() {
        return str;
    }

    int getI() {
        return i;
    }

    long getL() {
        return l;
    }

    double getD() {
        return d;
    }

    long getDate() {
        return date;
    }

    public void setId(final ObjectId id) {
        this.id = id;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Concrete concrete = (Concrete) o;

        if (Double.compare(concrete.d, d) != 0) {
            return false;
        }
        if (date != concrete.date) {
            return false;
        }
        if (i != concrete.i) {
            return false;
        }
        if (l != concrete.l) {
            return false;
        }
        if (!id.equals(concrete.id)) {
            return false;
        }
        if (!str.equals(concrete.str)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id.hashCode();
        result = 31 * result + str.hashCode();
        result = 31 * result + i;
        result = 31 * result + (int) (l ^ (l >>> 32));
        temp = Double.doubleToLongBits(d);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (date ^ (date >>> 32));
        return result;
    }
}
