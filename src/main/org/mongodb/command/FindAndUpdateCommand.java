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

import org.mongodb.MongoClient;
import org.mongodb.MongoCommandDocument;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFindAndUpdate;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.PrimitiveSerializers;

public class FindAndUpdateCommand<T> extends FindAndModifyCommand<T> {
    private final MongoFindAndUpdate findAndUpdate;
    public FindAndUpdateCommand(final MongoClient mongoClient, final MongoNamespace namespace,
                                final MongoFindAndUpdate findAndUpdate, PrimitiveSerializers primitiveSerializers, Serializer<T> serializer) {
        super(mongoClient, namespace, findAndUpdate, primitiveSerializers, serializer);
        this.findAndUpdate = findAndUpdate;
    }

    @Override
    public MongoCommand asMongoCommand() {
        MongoCommandDocument cmd = getBaseCommandDocument();
        cmd.put("update", findAndUpdate.getUpdateOperations());
        return cmd;
    }
}
