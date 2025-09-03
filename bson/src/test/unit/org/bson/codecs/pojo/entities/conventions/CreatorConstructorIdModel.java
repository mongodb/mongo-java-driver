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
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.List;

public class CreatorConstructorIdModel {
  private final String id;
  private final List<Integer> integersField;
  private String stringField;
  public long longField;

  @BsonCreator
  public CreatorConstructorIdModel(@BsonId final String id, @BsonProperty("integersField") final List<Integer> integerField,
                                   @BsonProperty("longField") final long longField) {
    this.id = id;
    this.integersField = integerField;
    this.longField = longField;
  }

  public CreatorConstructorIdModel(final String id, final List<Integer> integersField, final String stringField, final long longField) {
    this.id = id;
    this.integersField = integersField;
    this.stringField = stringField;
    this.longField = longField;
  }

  public String getId() {
    return id;
  }

  public List<Integer> getIntegersField() {
    return integersField;
  }

  public String getStringField() {
    return stringField;
  }

  public void setStringField(final String stringField) {
    this.stringField = stringField;
  }

  public long getLongField() {
    return longField;
  }

  public void setLongField(final long longField) {
    this.longField = longField;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CreatorConstructorIdModel that = (CreatorConstructorIdModel) o;

    if (getLongField() != that.getLongField()) {
      return false;
    }
    if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) {
      return false;
    }
    if (getIntegersField() != null ? !getIntegersField().equals(that.getIntegersField())
        : that.getIntegersField() != null) {
      return false;
    }
    return getStringField() != null ? getStringField().equals(that.getStringField()) : that.getStringField() == null;
  }

  @Override
  public int hashCode() {
    int result = getId() != null ? getId().hashCode() : 0;
    result = 31 * result + (getIntegersField() != null ? getIntegersField().hashCode() : 0);
    result = 31 * result + (getStringField() != null ? getStringField().hashCode() : 0);
    result = 31 * result + (int) (getLongField() ^ (getLongField() >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "CreatorConstructorIdModel{"
        + "id='" + id + '\''
        + ", integersField=" + integersField
        + ", stringField='" + stringField + '\''
        + ", longField=" + longField
        + '}';
  }
}
