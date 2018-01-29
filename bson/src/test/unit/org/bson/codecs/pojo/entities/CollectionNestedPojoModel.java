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

package org.bson.codecs.pojo.entities;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;

public final class CollectionNestedPojoModel {

    @SuppressWarnings("checkstyle:name")
    private static List<SimpleModel> staticSimple = singletonList(new SimpleModel(1, "static"));
    private List<SimpleModel> listSimple;
    private List<List<SimpleModel>> listListSimple;

    private Set<SimpleModel> setSimple;
    private Set<Set<SimpleModel>> setSetSimple;

    private Map<String, SimpleModel> mapSimple;
    private Map<String, Map<String, SimpleModel>> mapMapSimple;

    private Map<String, List<SimpleModel>> mapListSimple;
    private Map<String, List<Map<String, SimpleModel>>> mapListMapSimple;
    private Map<String, Set<SimpleModel>> mapSetSimple;

    private List<Map<String, SimpleModel>> listMapSimple;
    private List<Map<String, List<SimpleModel>>> listMapListSimple;
    private List<Map<String, Set<SimpleModel>>> listMapSetSimple;

    public CollectionNestedPojoModel() {
    }

    public CollectionNestedPojoModel(final List<SimpleModel> listSimple, final List<List<SimpleModel>> listListSimple, final
    Set<SimpleModel> setSimple, final Set<Set<SimpleModel>> setSetSimple, final Map<String, SimpleModel> mapSimple, final Map<String,
            Map<String, SimpleModel>> mapMapSimple, final Map<String, List<SimpleModel>> mapListSimple, final Map<String,
            List<Map<String, SimpleModel>>> mapListMapSimple, final Map<String, Set<SimpleModel>> mapSetSimple, final List<Map<String,
            SimpleModel>> listMapSimple, final List<Map<String, List<SimpleModel>>> listMapListSimple, final List<Map<String,
            Set<SimpleModel>>> listMapSetSimple) {
        this.listSimple = listSimple;
        this.listListSimple = listListSimple;
        this.setSimple = setSimple;
        this.setSetSimple = setSetSimple;
        this.mapSimple = mapSimple;
        this.mapMapSimple = mapMapSimple;
        this.mapListSimple = mapListSimple;
        this.mapListMapSimple = mapListMapSimple;
        this.mapSetSimple = mapSetSimple;
        this.listMapSimple = listMapSimple;
        this.listMapListSimple = listMapListSimple;
        this.listMapSetSimple = listMapSetSimple;
    }

    public static List<SimpleModel> getStaticSimple() {
        return staticSimple;
    }

    public static void setStaticSimple(final List<SimpleModel> staticSimple) {
        CollectionNestedPojoModel.staticSimple = staticSimple;
    }

    public List<SimpleModel> getListSimple() {
        return listSimple;
    }

    public void setListSimple(final List<SimpleModel> listSimple) {
        this.listSimple = listSimple;
    }

    public List<List<SimpleModel>> getListListSimple() {
        return listListSimple;
    }

    public void setListListSimple(final List<List<SimpleModel>> listListSimple) {
        this.listListSimple = listListSimple;
    }

    public Set<SimpleModel> getSetSimple() {
        return setSimple;
    }

    public void setSetSimple(final Set<SimpleModel> setSimple) {
        this.setSimple = setSimple;
    }

    public Set<Set<SimpleModel>> getSetSetSimple() {
        return setSetSimple;
    }

    public void setSetSetSimple(final Set<Set<SimpleModel>> setSetSimple) {
        this.setSetSimple = setSetSimple;
    }

    public Map<String, SimpleModel> getMapSimple() {
        return mapSimple;
    }

    public void setMapSimple(final Map<String, SimpleModel> mapSimple) {
        this.mapSimple = mapSimple;
    }

    public Map<String, Map<String, SimpleModel>> getMapMapSimple() {
        return mapMapSimple;
    }

    public void setMapMapSimple(final Map<String, Map<String, SimpleModel>> mapMapSimple) {
        this.mapMapSimple = mapMapSimple;
    }

    public Map<String, List<SimpleModel>> getMapListSimple() {
        return mapListSimple;
    }

    public void setMapListSimple(final Map<String, List<SimpleModel>> mapListSimple) {
        this.mapListSimple = mapListSimple;
    }

    public Map<String, List<Map<String, SimpleModel>>> getMapListMapSimple() {
        return mapListMapSimple;
    }

    public void setMapListMapSimple(final Map<String, List<Map<String, SimpleModel>>> mapListMapSimple) {
        this.mapListMapSimple = mapListMapSimple;
    }

    public Map<String, Set<SimpleModel>> getMapSetSimple() {
        return mapSetSimple;
    }

    public void setMapSetSimple(final Map<String, Set<SimpleModel>> mapSetSimple) {
        this.mapSetSimple = mapSetSimple;
    }

    public List<Map<String, SimpleModel>> getListMapSimple() {
        return listMapSimple;
    }

    public void setListMapSimple(final List<Map<String, SimpleModel>> listMapSimple) {
        this.listMapSimple = listMapSimple;
    }

    public List<Map<String, List<SimpleModel>>> getListMapListSimple() {
        return listMapListSimple;
    }

    public void setListMapListSimple(final List<Map<String, List<SimpleModel>>> listMapListSimple) {
        this.listMapListSimple = listMapListSimple;
    }

    public List<Map<String, Set<SimpleModel>>> getListMapSetSimple() {
        return listMapSetSimple;
    }

    public void setListMapSetSimple(final List<Map<String, Set<SimpleModel>>> listMapSetSimple) {
        this.listMapSetSimple = listMapSetSimple;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CollectionNestedPojoModel that = (CollectionNestedPojoModel) o;

        if (getListSimple() != null ? !getListSimple().equals(that.getListSimple()) : that.getListSimple() != null) {
            return false;
        }
        if (getListListSimple() != null ? !getListListSimple().equals(that.getListListSimple()) : that.getListListSimple() != null) {
            return false;
        }
        if (getSetSimple() != null ? !getSetSimple().equals(that.getSetSimple()) : that.getSetSimple() != null) {
            return false;
        }
        if (getSetSetSimple() != null ? !getSetSetSimple().equals(that.getSetSetSimple()) : that.getSetSetSimple() != null) {
            return false;
        }
        if (getMapSimple() != null ? !getMapSimple().equals(that.getMapSimple()) : that.getMapSimple() != null) {
            return false;
        }
        if (getMapMapSimple() != null ? !getMapMapSimple().equals(that.getMapMapSimple()) : that.getMapMapSimple() != null) {
            return false;
        }
        if (getMapListSimple() != null ? !getMapListSimple().equals(that.getMapListSimple()) : that.getMapListSimple() != null) {
            return false;
        }
        if (getMapListMapSimple() != null ? !getMapListMapSimple().equals(that.getMapListMapSimple())
                : that.getMapListMapSimple() !=  null) {
            return false;
        }
        if (getMapSetSimple() != null ? !getMapSetSimple().equals(that.getMapSetSimple()) : that.getMapSetSimple() != null) {
            return false;
        }
        if (getListMapSimple() != null ? !getListMapSimple().equals(that.getListMapSimple()) : that.getListMapSimple() != null) {
            return false;
        }
        if (getListMapListSimple() != null ? !getListMapListSimple().equals(that.getListMapListSimple())
                : that.getListMapListSimple() != null) {
            return false;
        }
        if (getListMapSetSimple() != null ? !getListMapSetSimple().equals(that.getListMapSetSimple())
                : that.getListMapSetSimple() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getListSimple() != null ? getListSimple().hashCode() : 0;
        result = 31 * result + (getListListSimple() != null ? getListListSimple().hashCode() : 0);
        result = 31 * result + (getSetSimple() != null ? getSetSimple().hashCode() : 0);
        result = 31 * result + (getSetSetSimple() != null ? getSetSetSimple().hashCode() : 0);
        result = 31 * result + (getMapSimple() != null ? getMapSimple().hashCode() : 0);
        result = 31 * result + (getMapMapSimple() != null ? getMapMapSimple().hashCode() : 0);
        result = 31 * result + (getMapListSimple() != null ? getMapListSimple().hashCode() : 0);
        result = 31 * result + (getMapListMapSimple() != null ? getMapListMapSimple().hashCode() : 0);
        result = 31 * result + (getMapSetSimple() != null ? getMapSetSimple().hashCode() : 0);
        result = 31 * result + (getListMapSimple() != null ? getListMapSimple().hashCode() : 0);
        result = 31 * result + (getListMapListSimple() != null ? getListMapListSimple().hashCode() : 0);
        result = 31 * result + (getListMapSetSimple() != null ? getListMapSetSimple().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CollectionNestedPojoModel{"
                + "listSimple=" + listSimple
                + ", listListSimple=" + listListSimple
                + ", setSimple=" + setSimple
                + ", setSetSimple=" + setSetSimple
                + ", mapSimple=" + mapSimple
                + ", mapMapSimple=" + mapMapSimple
                + ", mapListSimple=" + mapListSimple
                + ", mapListMapSimple=" + mapListMapSimple
                + ", mapSetSimple=" + mapSetSimple
                + ", listMapSimple=" + listMapSimple
                + ", listMapListSimple=" + listMapListSimple
                + ", listMapSetSimple=" + listMapSetSimple
                + "}";
    }
}
