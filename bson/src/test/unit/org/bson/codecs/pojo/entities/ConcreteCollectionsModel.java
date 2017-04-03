/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo.entities;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ConcreteCollectionsModel {
    private Collection<Integer> collection;
    private List<Integer> list;
    private LinkedList<Integer> linked;
    private Map<String, Double> map;
    private ConcurrentHashMap<String, Double> concurrent;

    public ConcreteCollectionsModel() {
    }

    public ConcreteCollectionsModel(final Collection<Integer> collection, final List<Integer> list, final LinkedList<Integer> linked,
                                    final Map<String, Double> map, final ConcurrentHashMap<String, Double> concurrent) {
        this.collection = collection;
        this.list = list;
        this.linked = linked;
        this.map = map;
        this.concurrent = concurrent;
    }

    public Collection<Integer> getCollection() {
        return collection;
    }

    public void setCollection(final Collection<Integer> collection) {
        this.collection = collection;
    }

    public List<Integer> getList() {
        return list;
    }

    public void setList(final List<Integer> list) {
        this.list = list;
    }

    public LinkedList<Integer> getLinked() {
        return linked;
    }

    public void setLinked(final LinkedList<Integer> linked) {
        this.linked = linked;
    }

    public Map<String, Double> getMap() {
        return map;
    }

    public void setMap(final Map<String, Double> map) {
        this.map = map;
    }

    public ConcurrentHashMap<String, Double> getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(final ConcurrentHashMap<String, Double> concurrent) {
        this.concurrent = concurrent;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConcreteCollectionsModel that = (ConcreteCollectionsModel) o;

        if (getCollection() != null ? !getCollection().equals(that.getCollection()) : that.getCollection() != null) {
            return false;
        }
        if (getList() != null ? !getList().equals(that.getList()) : that.getList() != null) {
            return false;
        }
        if (getLinked() != null ? !getLinked().equals(that.getLinked()) : that.getLinked() != null) {
            return false;
        }
        if (getMap() != null ? !getMap().equals(that.getMap()) : that.getMap() != null) {
            return false;
        }
        if (getConcurrent() != null ? !getConcurrent().equals(that.getConcurrent()) : that.getConcurrent() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getCollection() != null ? getCollection().hashCode() : 0;
        result = 31 * result + (getList() != null ? getList().hashCode() : 0);
        result = 31 * result + (getLinked() != null ? getLinked().hashCode() : 0);
        result = 31 * result + (getMap() != null ? getMap().hashCode() : 0);
        result = 31 * result + (getConcurrent() != null ? getConcurrent().hashCode() : 0);
        return result;
    }
}
