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
import com.mongodb.internal.client.model.bulk.ConcreteClientReplaceOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientWriteModelWithNamespace;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An individual write operation to be executed as part of a client-level bulk write operation.
 *
 * @since 5.3
 */
@Sealed
public interface ClientWriteModel {
    /**
     * Creates a model for inserting the {@code document}.
     *
     * @param document The document.
     * @return The requested model.
     * @param <TDocument> The document type, for example {@link Document}.
     * @see Filters
     */
    static <TDocument> ClientInsertOneModel insertOne(final TDocument document) {
        notNull("document", document);
        return new ConcreteClientInsertOneModel(document);
    }

    /**
     * Creates a model for updating at most one document matching the {@code filter}.
     * This method is functionally equivalent to {@link #updateOne(Bson, Bson, ClientUpdateOptions)}
     * with the {@linkplain ClientUpdateOptions#clientUpdateOptions() default options}.
     *
     * @param filter The filter.
     * @param update The update.
     * @return The requested model.
     * @see Filters
     * @see Updates
     */
    static ClientUpdateOneModel updateOne(final Bson filter, final Bson update) {
        notNull("filter", filter);
        notNull("update", update);
        return new ConcreteClientUpdateOneModel(filter, update, null, null);
    }

    /**
     * Creates a model for updating at most one document matching the {@code filter}.
     *
     * @param filter The filter.
     * @param update The update.
     * @param options The options.
     * @return The requested model.
     * @see Filters
     * @see Updates
     */
    static ClientUpdateOneModel updateOne(final Bson filter, final Bson update, final ClientUpdateOptions options) {
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return new ConcreteClientUpdateOneModel(filter, update, null, options);
    }

    /**
     * Creates a model for updating at most one document matching the {@code filter}.
     * This method is functionally equivalent to {@link #updateOne(Bson, Iterable, ClientUpdateOptions)}
     * with the {@linkplain ClientUpdateOptions#clientUpdateOptions() default options}.
     *
     * @param filter The filter.
     * @param updatePipeline The update pipeline.
     * @return The requested model.
     * @see Filters
     * @see Aggregates
     */
    static ClientUpdateOneModel updateOne(final Bson filter, final Iterable<? extends Bson> updatePipeline) {
        notNull("filter", filter);
        notNull("updatePipeline", updatePipeline);
        return new ConcreteClientUpdateOneModel(filter, null, updatePipeline, null);
    }

    /**
     * Creates a model for updating at most one document matching the {@code filter}.
     *
     * @param filter The filter.
     * @param updatePipeline The update pipeline.
     * @param options The options.
     * @return The requested model.
     * @see Filters
     * @see Aggregates
     */
    static ClientUpdateOneModel updateOne(final Bson filter, final Iterable<? extends Bson> updatePipeline, final ClientUpdateOptions options) {
        notNull("filter", filter);
        notNull("updatePipeline", updatePipeline);
        notNull("options", options);
        return new ConcreteClientUpdateOneModel(filter, null, updatePipeline, options);
    }

    /**
     * Creates a model for updating all documents matching the {@code filter}.
     * This method is functionally equivalent to {@link #updateMany(Bson, Bson, ClientUpdateOptions)}
     * with the {@linkplain ClientUpdateOptions#clientUpdateOptions() default}.
     *
     * @param filter The filter.
     * @param update The update.
     * @return The requested model.
     * @see Filters
     * @see Updates
     */
    static ClientUpdateManyModel updateMany(final Bson filter, final Bson update) {
        notNull("filter", filter);
        notNull("update", update);
        return new ConcreteClientUpdateManyModel(filter, update, null, null);
    }

    /**
     * Creates a model for updating all documents matching the {@code filter}.
     *
     * @param filter The filter.
     * @param update The update.
     * @param options The options.
     * @return The requested model.
     * @see Filters
     * @see Updates
     */
    static ClientUpdateManyModel updateMany(final Bson filter, final Bson update, final ClientUpdateOptions options) {
        notNull("filter", filter);
        notNull("update", update);
        notNull("options", options);
        return new ConcreteClientUpdateManyModel(filter, update, null, options);
    }

    /**
     * Creates a model for updating all documents matching the {@code filter}.
     * This method is functionally equivalent to {@link #updateMany(Bson, Iterable, ClientUpdateOptions)}
     * with the {@linkplain ClientUpdateOptions#clientUpdateOptions() default options}.
     *
     * @param filter The filter.
     * @param updatePipeline The update pipeline.
     * @return The requested model.
     * @see Filters
     * @see Aggregates
     */
    static ClientUpdateManyModel updateMany(final Bson filter, final Iterable<? extends Bson> updatePipeline) {
        notNull("filter", filter);
        notNull("updatePipeline", updatePipeline);
        return new ConcreteClientUpdateManyModel(filter, null, updatePipeline, null);
    }

    /**
     * Creates a model for updating all documents matching the {@code filter}.
     *
     * @param filter The filter.
     * @param updatePipeline The update pipeline.
     * @param options The options.
     * @return The requested model.
     * @see Filters
     * @see Aggregates
     */
    static ClientUpdateManyModel updateMany(final Bson filter, final Iterable<? extends Bson> updatePipeline, final ClientUpdateOptions options) {
        notNull("filter", filter);
        notNull("updatePipeline", updatePipeline);
        notNull("options", options);
        return new ConcreteClientUpdateManyModel(filter, null, updatePipeline, options);
    }

    /**
     * Creates a model for replacing at most one document matching the {@code filter}.
     * This method is functionally equivalent to {@link #replaceOne(Bson, Object, ClientReplaceOptions)}
     * with the {@linkplain ClientReplaceOptions#clientReplaceOptions() default options}.
     *
     * @param filter The filter.
     * @param replacement The replacement.
     * The keys of this document must not start with {@code $}, unless they express a {@linkplain com.mongodb.DBRef database reference}.
     * @return The requested model.
     * @param <TDocument> The document type, for example {@link Document}.
     * @see Filters
     */
    static <TDocument> ClientReplaceOneModel replaceOne(final Bson filter, final TDocument replacement) {
        notNull("filter", filter);
        notNull("replacement", replacement);
        return new ConcreteClientReplaceOneModel(filter, replacement, null);
    }

    /**
     * Creates a model for replacing at most one document matching the {@code filter}.
     *
     * @param filter The filter.
     * @param replacement The replacement.
     * The keys of this document must not start with {@code $}, unless they express a {@linkplain com.mongodb.DBRef database reference}.
     * @param options The options.
     * @return The requested model.
     * @param <TDocument> The document type, for example {@link Document}.
     * @see Filters
     */
    static <TDocument> ClientReplaceOneModel replaceOne(final Bson filter, final TDocument replacement, final ClientReplaceOptions options) {
        notNull("filter", filter);
        notNull("replacement", replacement);
        notNull("options", options);
        return new ConcreteClientReplaceOneModel(filter, replacement, options);
    }

    /**
     * Creates a model for removing at most one document matching the {@code filter}.
     * This method is functionally equivalent to {@link #deleteOne(Bson, ClientDeleteOptions)}
     * with the {@linkplain ClientDeleteOptions#clientDeleteOptions() default options}.
     *
     * @param filter The filter.
     * @return The requested model.
     * @see Filters
     */
    static ClientDeleteOneModel deleteOne(final Bson filter) {
        notNull("filter", filter);
        return new ConcreteClientDeleteOneModel(filter, null);
    }

    /**
     * Creates a model for removing at most one document matching the {@code filter}.
     *
     * @param filter The filter.
     * @param options The options.
     * @return The requested model.
     * @see Filters
     */
    static ClientDeleteOneModel deleteOne(final Bson filter, final ClientDeleteOptions options) {
        notNull("filter", filter);
        notNull("options", options);
        return new ConcreteClientDeleteOneModel(filter, options);
    }

    /**
     * Creates a model for removing all documents matching the {@code filter}.
     * This method is functionally equivalent to {@link #deleteMany(Bson, ClientDeleteOptions)}
     * with the {@linkplain ClientDeleteOptions#clientDeleteOptions() default options}.
     *
     * @param filter The filter.
     * @return The requested model.
     * @see Filters
     */
    static ClientDeleteManyModel deleteMany(final Bson filter) {
        notNull("filter", filter);
        return new ConcreteClientDeleteManyModel(filter, null);
    }

    /**
     * Creates a model for removing all documents matching the {@code filter}.
     *
     * @param filter The filter.
     * @param options The options.
     * @return The requested model.
     * @see Filters
     */
    static ClientDeleteManyModel deleteMany(final Bson filter, final ClientDeleteOptions options) {
        notNull("filter", filter);
        notNull("options", options);
        return new ConcreteClientDeleteManyModel(filter, options);
    }

    /**
     * Combines this model with the {@code namespace} it is targeted at.
     *
     * @param namespace The namespace.
     * @return The model with the {@code namespace}.
     */
    default ClientWriteModelWithNamespace withNamespace(final MongoNamespace namespace) {
        notNull("namespace", namespace);
        return new ConcreteClientWriteModelWithNamespace(this, namespace);
    }
}
