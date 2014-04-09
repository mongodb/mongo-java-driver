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

package org.mongodb.operation;

import org.mongodb.AggregationOptions;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.session.Session;

import java.util.List;

/**
 * An operation that executes an explain on an aggregation pipeline.
 *
 * @since 3.0
 */
public class AggregateExplainOperation extends AggregateBaseOperation<Document> implements Operation<CommandResult> {
    /**
     * Constructs a new instance.
     *
     * @param namespace the namespace
     * @param pipeline the aggregation pipeline
     * @param options the aggregation options
     * @param readPreference the read preference
     */
    public AggregateExplainOperation(final MongoNamespace namespace, final List<Document> pipeline, final AggregationOptions options,
                                     final ReadPreference readPreference) {
        super(namespace, pipeline, new DocumentCodec(), options, readPreference);
    }

    @Override
    public CommandResult execute(final Session session) {
        return sendAndReceiveMessage(session);
    }

    @Override
    protected Document asCommandDocument() {
        Document command = super.asCommandDocument();
        command.put("explain", true);
        return command;
    }
}
