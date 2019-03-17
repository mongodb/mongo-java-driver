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

package com.mongodb.client.model.changestream;

import com.mongodb.MongoNamespace;
import com.mongodb.assertions.Assertions;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

/**
 * Represents the {@code $changeStream} aggregation output document.
 *
 * <p>Note: this class will not be applicable for all change stream outputs. If using custom pipelines that radically change the
 * change stream result, then an alternative document format should be used.</p>
 *
 * @param <TDocument> The type that this collection will encode the {@code fullDocument} field into.
 * @since 3.6
 */
public final class ChangeStreamDocument<TDocument> {

    @BsonId()
    private final BsonDocument resumeToken;
    private final BsonDocument namespaceDocument;
    private final BsonDocument destinationNamespaceDocument;
    private final TDocument fullDocument;
    private final BsonDocument documentKey;
    private final BsonTimestamp clusterTime;
    private final OperationType operationType;
    private final UpdateDescription updateDescription;
    private final BsonInt64 txnNumber;
    private final BsonDocument lsid;

    /**
     * Creates a new instance
     *
     * @param operationType the operation type
     * @param resumeToken the resume token
     * @param namespaceDocument the BsonDocument representing the namespace
     * @param destinationNamespaceDocument the BsonDocument representing the destinatation namespace
     * @param fullDocument the full document
     * @param documentKey a document containing the _id of the changed document
     * @param clusterTime the cluster time at which the change occured
     * @param updateDescription the update description
     *
     * @since 3.11
     */
    @Deprecated
    public ChangeStreamDocument(@BsonProperty("operationType") final OperationType operationType,
                                @BsonProperty("resumeToken") final BsonDocument resumeToken,
                                @Nullable @BsonProperty("ns") final BsonDocument namespaceDocument,
                                @Nullable @BsonProperty("to") final BsonDocument destinationNamespaceDocument,
                                @Nullable @BsonProperty("fullDocument") final TDocument fullDocument,
                                @Nullable @BsonProperty("documentKey") final BsonDocument documentKey,
                                @Nullable @BsonProperty("clusterTime") final BsonTimestamp clusterTime,
                                @Nullable @BsonProperty("updateDescription") final UpdateDescription updateDescription) {
        this(operationType, resumeToken, namespaceDocument, destinationNamespaceDocument, fullDocument, documentKey, clusterTime,
                updateDescription, null, null);
    }

    /**
     * Creates a new instance
     *
     * @param operationType the operation type
     * @param resumeToken the resume token
     * @param namespaceDocument the BsonDocument representing the namespace
     * @param destinationNamespaceDocument the BsonDocument representing the destinatation namespace
     * @param fullDocument the full document
     * @param documentKey a document containing the _id of the changed document
     * @param clusterTime the cluster time at which the change occured
     * @param updateDescription the update description
     * @param txnNumber the transaction number
     * @param lsid the identifier for the session associated with the transaction
     *
     * @since 3.11
     */
    @BsonCreator
    public ChangeStreamDocument(@BsonProperty("operationType") final OperationType operationType,
                                @BsonProperty("resumeToken") final BsonDocument resumeToken,
                                @Nullable @BsonProperty("ns") final BsonDocument namespaceDocument,
                                @Nullable @BsonProperty("to") final BsonDocument destinationNamespaceDocument,
                                @Nullable @BsonProperty("fullDocument") final TDocument fullDocument,
                                @Nullable @BsonProperty("documentKey") final BsonDocument documentKey,
                                @Nullable @BsonProperty("clusterTime") final BsonTimestamp clusterTime,
                                @Nullable @BsonProperty("updateDescription") final UpdateDescription updateDescription,
                                @Nullable @BsonProperty("txnNumber") final BsonInt64 txnNumber,
                                @Nullable @BsonProperty("lsid") final BsonDocument lsid) {
        this.resumeToken = resumeToken;
        this.namespaceDocument = namespaceDocument;
        this.destinationNamespaceDocument = destinationNamespaceDocument;
        this.documentKey = documentKey;
        this.fullDocument = fullDocument;
        this.clusterTime = clusterTime;
        this.operationType = operationType;
        this.updateDescription = updateDescription;
        this.txnNumber = txnNumber;
        this.lsid = lsid;
    }

    private static BsonDocument namespaceToDocument(final MongoNamespace namespace) {
        Assertions.notNull("namespace", namespace);
        return new BsonDocument("db", new BsonString(namespace.getDatabaseName()))
                .append("coll", new BsonString(namespace.getCollectionName()));
    }

    /**
     * Returns the resumeToken
     *
     * @return the resumeToken
     */
    public BsonDocument getResumeToken() {
        return resumeToken;
    }

    /**
     * Returns the namespace, derived from the "ns" field in a change stream document.
     *
     * The invalidate operation type does include a MongoNamespace in the ChangeStreamDocument response. The
     * dropDatabase operation type includes a MongoNamespace, but does not include a collection name as part
     * of the namespace.
     *
     * @return the namespace. If the namespaceDocument is null or if it is missing either the 'db' or 'coll' keys,
     * then this will return null.
     */
    @BsonIgnore
    @Nullable
    public MongoNamespace getNamespace() {
        if (namespaceDocument == null) {
            return null;
        }
        if (!namespaceDocument.containsKey("db") || !namespaceDocument.containsKey("coll")) {
            return null;
        }

        return new MongoNamespace(namespaceDocument.getString("db").getValue(), namespaceDocument.getString("coll").getValue());
    }

    /**
     * Returns the namespace cocument, derived from the "ns" field in a change stream document.
     *
     * The namespace document is a BsonDocument containing the values associated with a MongoNamespace. The
     * 'db' key refers to the database name and the 'coll' key refers to the collection name.
     *
     * @return the namespaceDocument
     * @since 3.8
     */
    @BsonProperty("ns")
    @Nullable
    public BsonDocument getNamespaceDocument() {
        return namespaceDocument;
    }

    /**
     * Returns the destination namespace, derived from the "to" field in a change stream document.
     *
     * <p>
     * The destination namespace is used to indicate the destination of a collection rename event.
     * </p>
     *
     * @return the namespace. If the "to" document is null or absent, then this will return null.
     * @see OperationType#RENAME
     * @since 3.11
     */
    @BsonIgnore
    @Nullable
    public MongoNamespace getDestinationNamespace() {
        if (destinationNamespaceDocument == null) {
            return null;
        }

        return new MongoNamespace(destinationNamespaceDocument.getString("db").getValue(),
                destinationNamespaceDocument.getString("coll").getValue());
    }

    /**
     * Returns the destination namespace document, derived from the "to" field in a change stream document.
     *
     * <p>
     * The destination namespace document is a BsonDocument containing the values associated with a MongoNamespace. The
     * 'db' key refers to the database name and the 'coll' key refers to the collection name.
     * </p>
     * @return the destinationNamespaceDocument
     * @since 3.11
     */
    @BsonProperty("to")
    @Nullable
    public BsonDocument getDestinationNamespaceDocument() {
        return destinationNamespaceDocument;
    }

    /**
     * Returns the database name
     *
     * @return the databaseName. If the namespaceDocument is null or if it is missing the 'db' key, then this will
     * return null.
     * @since 3.8
     */
    @BsonIgnore
    @Nullable
    public String getDatabaseName() {
        if (namespaceDocument == null) {
            return null;
        }
        if (!namespaceDocument.containsKey("db")) {
            return null;
        }
        return namespaceDocument.getString("db").getValue();
    }

    /**
     * Returns the fullDocument
     *
     * @return the fullDocument
     */
    @Nullable
    public TDocument getFullDocument() {
        return fullDocument;
    }

    /**
     * Returns a document containing just the _id of the changed document.
     * <p>
     * For unsharded collections this contains a single field, _id, with the
     * value of the _id of the document updated.  For sharded collections,
     * this will contain all the components of the shard key in order,
     * followed by the _id if the _id isn’t part of the shard key.
     * </p>
     *
     * @return the document key, or null if the event is not associated with a single document (e.g. a collection rename event)
     */
    @Nullable
    public BsonDocument getDocumentKey() {
        return documentKey;
    }

    /**
     * Gets the cluster time at which the change occurred.
     *
     * @return the cluster time at which the change occurred
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    @Nullable
    public BsonTimestamp getClusterTime() {
        return clusterTime;
    }

    /**
     * Returns the operationType
     *
     * @return the operationType
     */
    public OperationType getOperationType() {
        return operationType;
    }

    /**
     * Returns the updateDescription
     *
     * @return the updateDescription, or null if the event is not associated with a single document (e.g. a collection rename event)
     */
    @Nullable
    public UpdateDescription getUpdateDescription() {
        return updateDescription;
    }

    /**
     * Returns the transaction number
     *
     * @return the transaction number, or null if not part of a multi-document transaction
     * @since 3.11
     * @mongodb.server.release 4.0
     */
    @Nullable
    public BsonInt64 getTxnNumber() {
        return txnNumber;
    }

    /**
     * Returns the identifier for the session associated with the transaction
     *
     * @return the lsid, or null if not part of a multi-document transaction
     * @since 3.11
     * @mongodb.server.release 4.0
     */
    @Nullable
    public BsonDocument getLsid() {
        return lsid;
    }

    /**
     * Creates the codec for the specific ChangeStreamOutput type
     *
     * @param fullDocumentClass the class to use to represent the fullDocument
     * @param codecRegistry the codec registry
     * @param <TFullDocument> the fullDocument type
     * @return the codec
     */
    public static <TFullDocument> Codec<ChangeStreamDocument<TFullDocument>> createCodec(final Class<TFullDocument> fullDocumentClass,
                                                                                         final CodecRegistry codecRegistry) {
        return new ChangeStreamDocumentCodec<TFullDocument>(fullDocumentClass, codecRegistry);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChangeStreamDocument<?> that = (ChangeStreamDocument<?>) o;

        if (resumeToken != null ? !resumeToken.equals(that.resumeToken) : that.resumeToken != null) {
            return false;
        }
        if (namespaceDocument != null ? !namespaceDocument.equals(that.namespaceDocument) : that.namespaceDocument != null) {
            return false;
        }
        if (destinationNamespaceDocument != null
                ? !destinationNamespaceDocument.equals(that.destinationNamespaceDocument)
                : that.destinationNamespaceDocument != null) {
            return false;
        }
        if (fullDocument != null ? !fullDocument.equals(that.fullDocument) : that.fullDocument != null) {
            return false;
        }
        if (documentKey != null ? !documentKey.equals(that.documentKey) : that.documentKey != null) {
            return false;
        }
        if (clusterTime != null ? !clusterTime.equals(that.clusterTime) : that.clusterTime != null) {
            return false;
        }
        if (operationType != that.operationType) {
            return false;
        }
        if (updateDescription != null ? !updateDescription.equals(that.updateDescription) : that.updateDescription != null) {
            return false;
        }
        if (txnNumber != null ? !txnNumber.equals(that.txnNumber) : that.txnNumber != null) {
            return false;
        }
        if (lsid != null ? !lsid.equals(that.lsid) : that.lsid != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = resumeToken != null ? resumeToken.hashCode() : 0;
        result = 31 * result + (namespaceDocument != null ? namespaceDocument.hashCode() : 0);
        result = 31 * result + (destinationNamespaceDocument != null ? destinationNamespaceDocument.hashCode() : 0);
        result = 31 * result + (fullDocument != null ? fullDocument.hashCode() : 0);
        result = 31 * result + (documentKey != null ? documentKey.hashCode() : 0);
        result = 31 * result + (clusterTime != null ? clusterTime.hashCode() : 0);
        result = 31 * result + (operationType != null ? operationType.hashCode() : 0);
        result = 31 * result + (updateDescription != null ? updateDescription.hashCode() : 0);
        result = 31 * result + (txnNumber != null ? txnNumber.hashCode() : 0);
        result = 31 * result + (lsid != null ? lsid.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ChangeStreamDocument{"
                + " operationType=" + operationType
                + ", resumeToken=" + resumeToken
                + ", namespace=" + getNamespace()
                + ", destinationNamespace=" + getDestinationNamespace()
                + ", fullDocument=" + fullDocument
                + ", documentKey=" + documentKey
                + ", clusterTime=" + clusterTime
                + ", updateDescription=" + updateDescription
                + ", txnNumber=" + txnNumber
                + ", lsid=" + lsid
                + "}";
    }
}
