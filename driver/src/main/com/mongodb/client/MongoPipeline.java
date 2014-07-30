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

import org.mongodb.Document;

public interface MongoPipeline<T> extends MongoIterable<T> {
    MongoPipeline<T> find(Document criteria);

    MongoPipeline<T> sort(Document id);

    MongoPipeline<T> skip(long skip);

    MongoPipeline<T> limit(long limit);

    MongoPipeline<T> project(Document projection);

    MongoPipeline<T> group(Document group);

    MongoPipeline<T> unwind(String field);
}
