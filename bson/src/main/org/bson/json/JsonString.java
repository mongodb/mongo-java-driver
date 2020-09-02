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

package org.bson.json;

/**
 * A wrapper class that holds a JSON string. This class makes decoding straight
 * to JSON easy.
 *
 * @since 4.2
 */
public class JsonString {
    private final String json;

    public JsonString(String json) {
        this.json = json;
    }

    public String getJson() {
        return json;
    }

    @Override
    public String toString() {
        return json;
    }
}
