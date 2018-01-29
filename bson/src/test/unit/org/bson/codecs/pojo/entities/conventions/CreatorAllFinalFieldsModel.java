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

package org.bson.codecs.pojo.entities.conventions;

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

@BsonDiscriminator
public final class CreatorAllFinalFieldsModel {
    private final String pid;
    private final String fName;
    private final String lName;

    @BsonCreator
    public CreatorAllFinalFieldsModel(@BsonProperty("personId") final String personId,
                                      @BsonProperty("firstName") final String firstName,
                                      @BsonProperty("lastName") final String lastName) {
        this.pid = personId;
        this.fName = firstName;
        this.lName = lastName;
    }

    @BsonId
    public String getPersonId() {
        return pid;
    }

    public String getFirstName() {
        return fName;
    }

    public String getLastName() {
        return lName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CreatorAllFinalFieldsModel that = (CreatorAllFinalFieldsModel) o;

        if (pid != null ? !pid.equals(that.pid) : that.pid != null) {
            return false;
        }
        if (fName != null ? !fName.equals(that.fName) : that.fName != null) {
            return false;
        }
        if (lName != null ? !lName.equals(that.lName) : that.lName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = pid != null ? pid.hashCode() : 0;
        result = 31 * result + (fName != null ? fName.hashCode() : 0);
        result = 31 * result + (lName != null ? lName.hashCode() : 0);
        return result;
    }
}
