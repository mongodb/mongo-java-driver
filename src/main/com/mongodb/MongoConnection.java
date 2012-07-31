/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb;

public class MongoConnection {

    public MongoConnection(final String namespace, final OutMessage.OpCode opCode, final String query) {
        this.namespace = namespace;
        this.opCode = opCode;
        this.query = query;
    }

    public String getNamespace() {
        return namespace;
    }

    public OutMessage.OpCode getOpCode() {
        return opCode;
    }

    public String getQuery() {
        return query;
    }

    private final String namespace;
    private final OutMessage.OpCode opCode;
    private final String query;

}
