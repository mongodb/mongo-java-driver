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

package org.bson.codecs.pojo;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

final class InvalidMapImplementation implements Map<Map<String, Integer>, Integer> {

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(final Object key) {
        return false;
    }

    @Override
    public boolean containsValue(final Object value) {
        return false;
    }

    @Override
    public Integer get(final Object key) {
        return null;
    }

    @Override
    public Integer put(final Map<String, Integer> key, final Integer value) {
        return null;
    }

    @Override
    public Integer remove(final Object key) {
        return null;
    }

    @Override
    public void putAll(final Map<? extends Map<String, Integer>, ? extends Integer> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<Map<String, Integer>> keySet() {
        return null;
    }

    @Override
    public Collection<Integer> values() {
        return null;
    }

    @Override
    public Set<Entry<Map<String, Integer>, Integer>> entrySet() {
        return null;
    }
}
