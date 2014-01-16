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

package org.mongodb.codecs.pojo;

import org.bson.BSONBinarySubType;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import java.util.Date;

public class AllPrimitiveTypes {

    private final ObjectId objectIdVal = new ObjectId(new Date(9876L));
    private final int intVal = 1;
    private final long longVal = 2L;
    private final String stringVal = "hello";
    private final double doubleVal = 3.2;
    private final Binary binaryVal = new Binary(BSONBinarySubType.USER_DEFINED, new byte[]{0, 1, 2, 3});
    private final Date jdkDateVal = new Date(1000);
    private final boolean booleanVal = true;
    private final Code codeVal = new Code("var i = 0");
    private final MinKey minKeyVal = new MinKey();

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AllPrimitiveTypes that = (AllPrimitiveTypes) o;

        if (booleanVal != that.booleanVal) {
            return false;
        }
        if (Double.compare(that.doubleVal, doubleVal) != 0) {
            return false;
        }
        if (intVal != that.intVal) {
            return false;
        }
        if (longVal != that.longVal) {
            return false;
        }
        if (!binaryVal.equals(that.binaryVal)) {
            return false;
        }
        if (!codeVal.equals(that.codeVal)) {
            return false;
        }
        if (!jdkDateVal.equals(that.jdkDateVal)) {
            return false;
        }
        if (!maxKeyVal.equals(that.maxKeyVal)) {
            return false;
        }
        if (!minKeyVal.equals(that.minKeyVal)) {
            return false;
        }
        if (nullVal != null ? !nullVal.equals(that.nullVal) : that.nullVal != null) {
            return false;
        }
        if (!objectIdVal.equals(that.objectIdVal)) {
            return false;
        }
        if (!stringVal.equals(that.stringVal)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = objectIdVal.hashCode();
        result = 31 * result + intVal;
        result = 31 * result + (int) (longVal ^ (longVal >>> 32));
        result = 31 * result + stringVal.hashCode();
        temp = Double.doubleToLongBits(doubleVal);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + binaryVal.hashCode();
        result = 31 * result + jdkDateVal.hashCode();
        result = 31 * result + (booleanVal ? 1 : 0);
        result = 31 * result + codeVal.hashCode();
        result = 31 * result + minKeyVal.hashCode();
        result = 31 * result + maxKeyVal.hashCode();
        result = 31 * result + (nullVal != null ? nullVal.hashCode() : 0);
        return result;
    }

    private final MaxKey maxKeyVal = new MaxKey();
    private final Object nullVal = null;

    @Override
    public String toString() {
        return "ObjectContainingAllPrimitiveTypes{"
               + "objectIdVal=" + objectIdVal
               + ", intVal=" + intVal
               + ", longVal=" + longVal
               + ", stringVal='" + stringVal + '\''
               + ", doubleVal=" + doubleVal
               + ", binaryVal=" + binaryVal
               + ", jdkDateVal=" + jdkDateVal
               + ", booleanVal=" + booleanVal
               + ", codeVal=" + codeVal
               + ", minKeyVal=" + minKeyVal
               + ", maxKeyVal=" + maxKeyVal
               + ", nullVal=" + nullVal
               + '}';
    }

}
