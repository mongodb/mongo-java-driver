/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.command;

import org.mongodb.CommandDocument;
import org.bson.types.Document;
import org.mongodb.MongoClient;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFindAndModify;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;

public abstract class FindAndModifyCommand<T> extends AbstractCommand {
    private final MongoNamespace namespace;
    private final MongoFindAndModify findAndModify;
    private final PrimitiveSerializers primitiveSerializers;
    private final Serializer<T> serializer;

    public FindAndModifyCommand(final MongoClient mongoClient, final MongoNamespace namespace,
                                final MongoFindAndModify findAndModify, final PrimitiveSerializers primitiveSerializers,
                                final Serializer<T> serializer) {
        super(mongoClient, namespace.getDatabaseName());
        this.namespace = namespace;
        this.findAndModify = findAndModify;
        this.primitiveSerializers = primitiveSerializers;
        this.serializer = serializer;
    }

    @Override
    public FindAndModifyCommandResult<T> execute() {
        return new FindAndModifyCommandResult<T>(getMongoClient().getOperations().executeCommand(getDatabase(),
                new MongoCommandOperation(asMongoCommand()),
                new FindAndModifyCommandResultSerializer<T>(primitiveSerializers, serializer)));

    }

    protected CommandDocument getBaseCommandDocument() {
        final CommandDocument cmd = new CommandDocument("findandmodify", namespace.getCollectionName());
        if (findAndModify.getFilter() != null) {
            cmd.put("query", findAndModify.getFilter());
        }
        if (findAndModify.getSelector() != null) {
            cmd.put("fields", findAndModify.getSelector());
        }
        if (findAndModify.getSortCriteria() != null) {
            cmd.put("sort", findAndModify.getSortCriteria());
        }
        if (findAndModify.isRemove()) {
            cmd.put("remove", true);
        }
        else {
            if (findAndModify.isReturnNew()) {
                cmd.put("new", true);
            }
            if (findAndModify.isUpsert()) {
                cmd.put("upsert", true);
            }
        }

        return cmd;
    }

    public static class FindAndModifyCommandResult<T> extends CommandResult {

        public FindAndModifyCommandResult(final Document document) {
            super(document);
        }

        public T getValue() {
            // TODO: any way to remove the warning?  This could be a design flaw
            return (T) getDocument().get("value");
        }
    }

    private class FindAndModifyCommandResultSerializer<T> extends DocumentSerializer {

        private final Serializer<T> serializer;

        public FindAndModifyCommandResultSerializer(final PrimitiveSerializers primitiveSerializers, final Serializer<T> serializer) {
            super(primitiveSerializers);
            this.serializer = serializer;
        }

        @Override
        protected Serializer getDocumentDeserializerForField(final String fieldName) {
            if (fieldName.equals("value")) {
                return serializer;
            }
            return new DocumentSerializer(getPrimitiveSerializers());
        }
    }
}
