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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.StreamProcessorInfo;
import com.mongodb.internal.MongoNamespaceHelper;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;

/**
 * Returns information about a single stream processor. This is a retryable read.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class GetStreamProcessorOperation implements ReadOperationSimple<StreamProcessorInfo> {
    private static final String COMMAND_NAME = "getStreamProcessor";
    private static final String DATABASE = "admin";
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();

    private final String processorName;
    private final boolean retryReads;

    public GetStreamProcessorOperation(final String processorName, final boolean retryReads) {
        this.processorName = notNull("processorName", processorName);
        this.retryReads = retryReads;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public MongoNamespace getNamespace() {
        return MongoNamespaceHelper.ADMIN_DB_COMMAND_NAMESPACE;
    }

    @Override
    public StreamProcessorInfo execute(final ReadBinding binding, final OperationContext operationContext) {
        return executeRetryableRead(binding, operationContext, DATABASE, getCommandCreator(), DECODER, transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final OperationContext operationContext,
                             final SingleResultCallback<StreamProcessorInfo> callback) {
        executeRetryableReadAsync(binding, operationContext, DATABASE, getCommandCreator(), DECODER, asyncTransformer(), retryReads, callback);
    }

    private SyncOperationHelper.CommandReadTransformer<BsonDocument, StreamProcessorInfo> transformer() {
        return (result, source, connection, operationContext) -> toStreamProcessorInfo(result);
    }

    private AsyncOperationHelper.CommandReadTransformerAsync<BsonDocument, StreamProcessorInfo> asyncTransformer() {
        return (result, source, connection, operationContext) -> toStreamProcessorInfo(result);
    }

    private CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) ->
                new BsonDocument(COMMAND_NAME, new BsonString(processorName));
    }

    private static StreamProcessorInfo toStreamProcessorInfo(final BsonDocument response) {
        try {
            // The server wraps the processor data in a "result" subdocument
            BsonDocument doc = response.containsKey("result") ? response.getDocument("result") : response;

            String id;
            if (doc.containsKey("_id")) {
                BsonValue rawId = doc.get("_id");
                id = rawId.isString() ? rawId.asString().getValue() : rawId.asObjectId().getValue().toHexString();
            } else if (doc.containsKey("id")) {
                id = doc.getString("id").getValue();
            } else {
                id = "";
            }

            StreamProcessorInfo.Builder builder = StreamProcessorInfo.builder()
                    .id(id)
                    .name(doc.getString("name").getValue())
                    .state(doc.getString("state").getValue())
                    .pipeline(toBsonDocumentList(doc.getArray("pipeline", new BsonArray())))
                    .pipelineVersion(doc.containsKey("pipelineVersion") ? doc.getInt32("pipelineVersion").getValue() : 0)
                    .enableAutoScaling(doc.containsKey("enableAutoScaling") && doc.getBoolean("enableAutoScaling").getValue())
                    .failoverEnabled(doc.containsKey("failoverEnabled") && doc.getBoolean("failoverEnabled").getValue())
                    .hasStarted(doc.containsKey("hasStarted") && doc.getBoolean("hasStarted").getValue())
                    .errorRetryable(doc.containsKey("errorRetryable") && doc.getBoolean("errorRetryable").getValue());

            if (doc.containsKey("tenantID") && !doc.isNull("tenantID")) {
                builder.tenantId(doc.getString("tenantID").getValue());
            }
            builder.activeRegion(doc.containsKey("activeRegion") && !doc.isNull("activeRegion")
                    ? doc.getString("activeRegion").getValue() : "");
            builder.workspaceDefaultRegion(doc.containsKey("workspaceDefaultRegion") && !doc.isNull("workspaceDefaultRegion")
                    ? doc.getString("workspaceDefaultRegion").getValue() : "");
            builder.modifiedBy(doc.containsKey("modifiedBy") && !doc.isNull("modifiedBy")
                    ? doc.getString("modifiedBy").getValue() : "");
            builder.errorMsg(doc.containsKey("errorMsg") && !doc.isNull("errorMsg")
                    ? doc.getString("errorMsg").getValue() : "");
            if (doc.containsKey("tier") && !doc.isNull("tier")) {
                builder.tier(doc.getString("tier").getValue());
            }
            if (doc.containsKey("dlq") && doc.isDocument("dlq")) {
                builder.dlq(doc.getDocument("dlq"));
            }
            if (doc.containsKey("streamMetaFieldName") && !doc.isNull("streamMetaFieldName")) {
                builder.streamMetaFieldName(doc.getString("streamMetaFieldName").getValue());
            }
            if (doc.containsKey("lastStateChange") && doc.isDateTime("lastStateChange")) {
                builder.lastStateChange(new Date(doc.getDateTime("lastStateChange").getValue()));
            }
            if (doc.containsKey("lastModifiedAt") && doc.isDateTime("lastModifiedAt")) {
                builder.lastModifiedAt(new Date(doc.getDateTime("lastModifiedAt").getValue()));
            }
            if (doc.containsKey("errorCode") && doc.isInt32("errorCode")) {
                builder.errorCode(doc.getInt32("errorCode").getValue());
            }

            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse getStreamProcessor response. Raw document: " + response.toJson(), e);
        }
    }

    private static List<BsonDocument> toBsonDocumentList(final BsonArray array) {
        List<BsonDocument> list = new ArrayList<>(array.size());
        for (BsonValue value : array) {
            list.add(value.asDocument());
        }
        return list;
    }

}
