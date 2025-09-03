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

package com.mongodb.internal.operation;

import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.SearchIndexModel;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class IndexHelper {

    /**
     * Get a list of index names for the given list of index models
     *
     * @param indexes the index models
     * @param codecRegistry the codec registry to convert each Bson key to a BsonDocument
     * @return the list of index names
     */
    public static List<String> getIndexNames(final List<IndexModel> indexes, final CodecRegistry codecRegistry) {
        List<String> indexNames = new ArrayList<>(indexes.size());
        for (IndexModel index : indexes) {
            String name = index.getOptions().getName();
            if (name != null) {
                indexNames.add(name);
            } else {
                indexNames.add(IndexHelper.generateIndexName(index.getKeys().toBsonDocument(BsonDocument.class, codecRegistry)));
            }
        }
        return indexNames;
    }

    /**
     * Get a list of Atlas Search index names for the given list of {@link SearchIndexModel}.
     *
     * @param indexes the search index models.
     * @return the list of search index names.
     */
    public static List<String> getSearchIndexNames(final List<SearchIndexModel> indexes) {
        return indexes.stream()
                .map(IndexHelper::getSearchIndexName)
                .collect(Collectors.toList());
    }

    private static String getSearchIndexName(final SearchIndexModel model) {
        String name = model.getName();
        return  name != null ? name : "default";
    }

    /**
     * Convenience method to generate an index name from the set of fields it is over.
     *
     * @return a string representation of this index's fields
     */
    public static String generateIndexName(final BsonDocument index) {
        StringBuilder indexName = new StringBuilder();
        for (final String keyNames : index.keySet()) {
            if (indexName.length() != 0) {
                indexName.append('_');
            }
            indexName.append(keyNames).append('_');
            BsonValue ascOrDescValue = index.get(keyNames);
            if (ascOrDescValue instanceof BsonNumber) {
                indexName.append(((BsonNumber) ascOrDescValue).intValue());
            } else if (ascOrDescValue instanceof BsonString) {
                indexName.append(((BsonString) ascOrDescValue).getValue().replace(' ', '_'));
            }
        }
        return indexName.toString();
    }

    private IndexHelper() {
    }
}
