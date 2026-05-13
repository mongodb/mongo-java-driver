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

    private static StreamProcessorInfo toStreamProcessorInfo(final BsonDocument result) {
        StreamProcessorInfo.Builder builder = StreamProcessorInfo.builder()
                .id(result.getString("id").getValue())
                .name(result.getString("name").getValue())
                .state(result.getString("state").getValue())
                .pipeline(toBsonDocumentList(result.getArray("pipeline", new BsonArray())))
                .pipelineVersion(result.getInt32("pipelineVersion").getValue())
                .enableAutoScaling(result.getBoolean("enableAutoScaling").getValue())
                .failoverEnabled(result.getBoolean("failoverEnabled").getValue())
                .activeRegion(result.getString("activeRegion").getValue())
                .workspaceDefaultRegion(result.getString("workspaceDefaultRegion").getValue())
                .modifiedBy(result.getString("modifiedBy").getValue())
                .hasStarted(result.getBoolean("hasStarted").getValue())
                .errorMsg(result.getString("errorMsg").getValue())
                .errorRetryable(result.getBoolean("errorRetryable").getValue());

        if (result.containsKey("tier") && !result.isNull("tier")) {
            builder.tier(result.getString("tier").getValue());
        }
        if (result.containsKey("dlq") && result.isDocument("dlq")) {
            builder.dlq(result.getDocument("dlq"));
        }
        if (result.containsKey("streamMetaFieldName") && !result.isNull("streamMetaFieldName")) {
            builder.streamMetaFieldName(result.getString("streamMetaFieldName").getValue());
        }
        if (result.containsKey("lastStateChange") && result.isDateTime("lastStateChange")) {
            builder.lastStateChange(new Date(result.getDateTime("lastStateChange").getValue()));
        }
        if (result.containsKey("lastModifiedAt") && result.isDateTime("lastModifiedAt")) {
            builder.lastModifiedAt(new Date(result.getDateTime("lastModifiedAt").getValue()));
        }
        if (result.containsKey("errorCode") && result.isInt32("errorCode")) {
            builder.errorCode(result.getInt32("errorCode").getValue());
        }

        return builder.build();
    }

    private static List<BsonDocument> toBsonDocumentList(final BsonArray array) {
        List<BsonDocument> list = new ArrayList<>(array.size());
        for (BsonValue value : array) {
            list.add(value.asDocument());
        }
        return list;
    }

}
