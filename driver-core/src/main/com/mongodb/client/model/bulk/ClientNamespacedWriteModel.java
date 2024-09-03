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

package com.mongodb.client.model.bulk;

import com.mongodb.MongoNamespace;
import com.mongodb.annotations.Sealed;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientInsertOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientNamespacedWriteModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientReplaceOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A combination of an individual write operation and a {@linkplain MongoNamespace namespace}
 * the operation is targeted at.
 *
 * @since 5.3
 */
@Sealed
public interface ClientNamespacedWriteModel {
    /**
     * Creates a model for inserting the {@code document} into the {@code namespace}.
     *
     * @param namespace The namespace.
     * @param document The document.
     * @return The requested {@link ClientNamespacedInsertOneModel}.
     * @param <TDocument> The document type, for example {@link Document}.
     * @see Filters
     */
    static <TDocument> ClientNamespacedInsertOneModel insertOne(final MongoNamespace namespace, final TDocument document) {
        notNull("namespace", namespace);
        notNull("document", document);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientInsertOneModel(document));
    }

    /**
     * Creates a model for updating at most one document in the {@code namespace} matching the {@code filter}.
     * This method is functionally equivalent to {@link #updateOne(MongoNamespace, Bson, Bson, ClientUpdateOptions)}
     * with the {@linkplain ClientUpdateOptions#clientUpdateOptions() default options}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param update The update.
     * @return The requested {@link ClientNamespacedUpdateOneModel}.
     * @see Filters
     * @see Updates
     */
    static ClientNamespacedUpdateOneModel updateOne(final MongoNamespace namespace, final Bson filter, final Bson update) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("update", update);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientUpdateOneModel(filter, update, null, null));
    }

    /**
     * Creates a model for updating at most one document in the {@code namespace} matching the {@code filter}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param update The update.
     * @param options The options.
     * @return The requested {@link ClientNamespacedUpdateOneModel}.
     * @see Filters
     * @see Updates
     */
    static ClientNamespacedUpdateOneModel updateOne(
            final MongoNamespace namespace, final Bson filter, final Bson update, final ClientUpdateOptions options) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientUpdateOneModel(filter, update, null, options));
    }

    /**
     * Creates a model for updating at most one document in the {@code namespace} matching the {@code filter}.
     * This method is functionally equivalent to {@link #updateOne(MongoNamespace, Bson, Iterable, ClientUpdateOptions)}
     * with the {@linkplain ClientUpdateOptions#clientUpdateOptions() default options}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param updatePipeline The update pipeline.
     * @return The requested {@link ClientNamespacedUpdateOneModel}.
     * @see Filters
     * @see Aggregates
     */
    static ClientNamespacedUpdateOneModel updateOne(
            final MongoNamespace namespace, final Bson filter, final Iterable<? extends Bson> updatePipeline) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("updatePipeline", updatePipeline);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientUpdateOneModel(filter, null, updatePipeline, null));
    }

    /**
     * Creates a model for updating at most one document in the {@code namespace} matching the {@code filter}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param updatePipeline The update pipeline.
     * @param options The options.
     * @return The requested {@link ClientNamespacedUpdateOneModel}.
     * @see Filters
     * @see Aggregates
     */
    static ClientNamespacedUpdateOneModel updateOne(
            final MongoNamespace namespace, final Bson filter, final Iterable<? extends Bson> updatePipeline, final ClientUpdateOptions options) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("updatePipeline", updatePipeline);
        notNull("options", options);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientUpdateOneModel(filter, null, updatePipeline, options));
    }

    /**
     * Creates a model for updating all documents in the {@code namespace} matching the {@code filter}.
     * This method is functionally equivalent to {@link #updateMany(MongoNamespace, Bson, Bson, ClientUpdateOptions)}
     * with the {@linkplain ClientUpdateOptions#clientUpdateOptions() default}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param update The update.
     * @return The requested {@link ClientNamespacedUpdateManyModel}.
     * @see Filters
     * @see Updates
     */
    static ClientNamespacedUpdateManyModel updateMany(final MongoNamespace namespace, final Bson filter, final Bson update) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("update", update);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientUpdateManyModel(filter, update, null, null));
    }

    /**
     * Creates a model for updating all documents in the {@code namespace} matching the {@code filter}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param update The update.
     * @param options The options.
     * @return The requested {@link ClientNamespacedUpdateManyModel}.
     * @see Filters
     * @see Updates
     */
    static ClientNamespacedUpdateManyModel updateMany(
            final MongoNamespace namespace, final Bson filter, final Bson update, final ClientUpdateOptions options) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientUpdateManyModel(filter, update, null, options));
    }

    /**
     * Creates a model for updating all documents in the {@code namespace} matching the {@code filter}.
     * This method is functionally equivalent to {@link #updateMany(MongoNamespace, Bson, Iterable, ClientUpdateOptions)}
     * with the {@linkplain ClientUpdateOptions#clientUpdateOptions() default options}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param updatePipeline The update pipeline.
     * @return The requested {@link ClientNamespacedUpdateManyModel}.
     * @see Filters
     * @see Aggregates
     */
    static ClientNamespacedUpdateManyModel updateMany(
            final MongoNamespace namespace, final Bson filter, final Iterable<? extends Bson> updatePipeline) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("updatePipeline", updatePipeline);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientUpdateManyModel(filter, null, updatePipeline, null));
    }

    /**
     * Creates a model for updating all documents in the {@code namespace} matching the {@code filter}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param updatePipeline The update pipeline.
     * @param options The options.
     * @return The requested {@link ClientNamespacedUpdateManyModel}.
     * @see Filters
     * @see Aggregates
     */
    static ClientNamespacedUpdateManyModel updateMany(
            final MongoNamespace namespace, final Bson filter, final Iterable<? extends Bson> updatePipeline, final ClientUpdateOptions options) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("updatePipeline", updatePipeline);
        notNull("options", options);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientUpdateManyModel(filter, null, updatePipeline, options));
    }

    /**
     * Creates a model for replacing at most one document in the {@code namespace} matching the {@code filter}.
     * This method is functionally equivalent to {@link #replaceOne(MongoNamespace, Bson, Object, ClientReplaceOptions)}
     * with the {@linkplain ClientReplaceOptions#clientReplaceOptions() default options}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param replacement The replacement.
     * The keys of this document must not start with {@code $}, unless they express a {@linkplain com.mongodb.DBRef database reference}.
     * @return The requested {@link ClientNamespacedReplaceOneModel}.
     * @param <TDocument> The document type, for example {@link Document}.
     * @see Filters
     */
    static <TDocument> ClientNamespacedReplaceOneModel replaceOne(final MongoNamespace namespace, final Bson filter, final TDocument replacement) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("replacement", replacement);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientReplaceOneModel(filter, replacement, null));
    }

    /**
     * Creates a model for replacing at most one document in the {@code namespace} matching the {@code filter}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param replacement The replacement.
     * The keys of this document must not start with {@code $}, unless they express a {@linkplain com.mongodb.DBRef database reference}.
     * @param options The options.
     * @return The requested {@link ClientNamespacedReplaceOneModel}.
     * @param <TDocument> The document type, for example {@link Document}.
     * @see Filters
     */
    static <TDocument> ClientNamespacedReplaceOneModel replaceOne(
            final MongoNamespace namespace, final Bson filter, final TDocument replacement, final ClientReplaceOptions options) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("replacement", replacement);
        notNull("options", options);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientReplaceOneModel(filter, replacement, options));
    }

    /**
     * Creates a model for deleting at most one document from the {@code namespace} matching the {@code filter}.
     * This method is functionally equivalent to {@link #deleteOne(MongoNamespace, Bson, ClientDeleteOptions)}
     * with the {@linkplain ClientDeleteOptions#clientDeleteOptions() default options}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @return The requested {@link ClientNamespacedDeleteOneModel}.
     * @see Filters
     */
    static ClientNamespacedDeleteOneModel deleteOne(final MongoNamespace namespace, final Bson filter) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientDeleteOneModel(filter, null));
    }

    /**
     * Creates a model for deleting at most one document from the {@code namespace} matching the {@code filter}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param options The options.
     * @return The requested {@link ClientNamespacedDeleteOneModel}.
     * @see Filters
     */
    static ClientNamespacedDeleteOneModel deleteOne(final MongoNamespace namespace, final Bson filter, final ClientDeleteOptions options) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("options", options);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientDeleteOneModel(filter, options));
    }

    /**
     * Creates a model for deleting all documents from the {@code namespace} matching the {@code filter}.
     * This method is functionally equivalent to {@link #deleteMany(MongoNamespace, Bson, ClientDeleteOptions)}
     * with the {@linkplain ClientDeleteOptions#clientDeleteOptions() default options}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @return The requested {@link ClientNamespacedDeleteManyModel}.
     * @see Filters
     */
    static ClientNamespacedDeleteManyModel deleteMany(final MongoNamespace namespace, final Bson filter) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientDeleteManyModel(filter, null));
    }

    /**
     * Creates a model for deleting all documents from the {@code namespace} matching the {@code filter}.
     *
     * @param namespace The namespace.
     * @param filter The filter.
     * @param options The options.
     * @return The requested {@link ClientNamespacedDeleteManyModel}.
     * @see Filters
     */
    static ClientNamespacedDeleteManyModel deleteMany(final MongoNamespace namespace, final Bson filter, final ClientDeleteOptions options) {
        notNull("namespace", namespace);
        notNull("filter", filter);
        notNull("options", options);
        return new ConcreteClientNamespacedWriteModel(namespace, new ConcreteClientDeleteManyModel(filter, options));
    }
}
