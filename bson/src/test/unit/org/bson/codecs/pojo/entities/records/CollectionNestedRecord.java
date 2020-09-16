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

package org.bson.codecs.pojo.entities.records;

import java.util.List;
import java.util.Map;
import java.util.Set;

public record CollectionNestedRecord(List<SimpleRecord> listSimple, List<List<SimpleRecord>> listListSimple,
                                     Set<SimpleRecord> setSimple, Set<Set<SimpleRecord>> setSetSimple, Map<String, SimpleRecord> mapSimple, Map<String,
                                     Map<String, SimpleRecord>> mapMapSimple, Map<String, List<SimpleRecord>> mapListSimple, Map<String,
                                     List<Map<String, SimpleRecord>>> mapListMapSimple, Map<String, Set<SimpleRecord>> mapSetSimple, List<Map<String,
                                     SimpleRecord>> listMapSimple, List<Map<String, List<SimpleRecord>>> listMapListSimple, List<Map<String,
                                     Set<SimpleRecord>>> listMapSetSimple) {
}
