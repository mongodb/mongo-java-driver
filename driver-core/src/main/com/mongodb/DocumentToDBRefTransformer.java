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

package com.mongodb;

import org.bson.Document;
import org.bson.Transformer;

/**
 * A Document to DBRef Transformer.
 * <p>
 * Can be used with any {@link org.bson.codecs.Codec} that takes a {@link Transformer}.
 *
 * @since 3.5
 */
public final class DocumentToDBRefTransformer implements Transformer {
    @Override
    public Object transform(final Object value) {
        if (value instanceof Document) {
            Document document = (Document) value;
            if (document.containsKey("$id") && document.containsKey("$ref")) {
                return new DBRef((String) document.get("$db"), (String) document.get("$ref"), document.get("$id"));
            }
        }
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
