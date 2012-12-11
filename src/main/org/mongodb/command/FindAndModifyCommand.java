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

import org.bson.BsonType;
import org.mongodb.MongoClient;
import org.mongodb.MongoCommandDocument;
import org.mongodb.MongoDocument;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFindAndModify;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.Serializers;
import org.mongodb.serialization.serializers.MongoDocumentSerializer;

public abstract class FindAndModifyCommand<T> extends AbstractCommand {
    private final MongoNamespace namespace;
    private final MongoFindAndModify findAndModify;
    private final Serializers serializers;

    public FindAndModifyCommand(final MongoClient mongoClient, final MongoNamespace namespace,
                                MongoFindAndModify findAndModify, Serializers serializers, Class<T> clazz) {
        super(mongoClient, namespace.getDatabaseName());
        this.namespace = namespace;
        this.findAndModify = findAndModify;
        this.serializers = createSerializers(serializers, clazz);
    }

    @Override
    public FindAndModifyCommandResult<T> execute() {
        return new FindAndModifyCommandResult<T>(getMongoClient().getOperations().executeCommand(getDatabase(),
                new MongoCommandOperation(asMongoCommand()), serializers));

    }

    private Serializers createSerializers(final Serializers serializers, final Class<T> clazz) {
        Serializers newSerializers = new Serializers(serializers);
        newSerializers.register(MongoDocument.class, BsonType.DOCUMENT,
                new FindAndModifyCommandResultSerializer<T>(newSerializers, clazz));
        return newSerializers;
    }

    protected MongoCommandDocument getBaseCommandDocument() {
        MongoCommandDocument cmd = new MongoCommandDocument("findandmodify", namespace.getCollectionName());
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
        } else {
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

        public FindAndModifyCommandResult(final MongoDocument mongoDocument) {
            super(mongoDocument);
        }

        // TODO: How will the Serializer know to deserialize value into a T and not a MongoDocument?  Custom serializer?
        public T getValue() {
            return (T) getMongoDocument().get("value");
        }
    }

    private class FindAndModifyCommandResultSerializer<T> extends MongoDocumentSerializer {

        private final Class<T> clazz;

        public FindAndModifyCommandResultSerializer(final Serializers serializers, Class<T> clazz) {
            super(serializers);
            this.clazz = clazz;
        }

        @Override
        protected Class getClassByBsonType(final BsonType bsonType, final String fieldName) {
            // TODO: this is a bug waiting to happen.  What if there are other fields named "value" in sub-documents?
            if (fieldName.equals("value")) {
                return clazz;
            }
            return super.getClassByBsonType(bsonType, fieldName);
        }
    }
}
