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

package com.mongodb.client.test;

import org.bson.BsonObjectId;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.ObjectId;

import java.util.Date;

public final class WorkerCodec implements CollectibleCodec<Worker> {
    @Override
    public boolean documentHasId(final Worker document) {
        return true;
    }

    @Override
    public BsonObjectId getDocumentId(final Worker document) {
        return new BsonObjectId(document.getId());
    }

    @Override
    public Worker generateIdIfAbsentFromDocument(final Worker worker) {
        return worker;
    }

    @Override
    public void encode(final BsonWriter writer, final Worker value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeObjectId("_id", value.getId());
        writer.writeString("name", value.getName());
        writer.writeString("jobTitle", value.getJobTitle());
        writer.writeDateTime("dateStarted", value.getDateStarted().getTime());
        writer.writeInt32("numberOfJobs", value.getNumberOfJobs());
        writer.writeEndDocument();
    }

    @Override
    public Worker decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartDocument();
        ObjectId id = reader.readObjectId("_id");
        String name = reader.readString("name");
        String jobTitle = reader.readString("jobTitle");
        Date dateStarted = new Date(reader.readDateTime("dateStarted"));
        int numberOfJobs = reader.readInt32("numberOfJobs");
        reader.readEndDocument();
        return new Worker(id, name, jobTitle, dateStarted, numberOfJobs);
    }

    @Override
    public Class<Worker> getEncoderClass() {
        return Worker.class;
    }
}
