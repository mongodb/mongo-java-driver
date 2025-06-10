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
import com.mongodb.lang.Nullable;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonExtraElements;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.util.Objects;

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

    @BsonProperty("nsType")
    private final String namespaceTypeString;
    @BsonIgnore
    private final NamespaceType namespaceType;
    private final BsonDocument destinationNamespaceDocument;
    private final TDocument fullDocument;
    private final TDocument fullDocumentBeforeChange;
    private final BsonDocument documentKey;
    private final BsonTimestamp clusterTime;
    @BsonProperty("operationType")
    private final String operationTypeString;
    @BsonIgnore
    private final OperationType operationType;
    private final UpdateDescription updateDescription;
    private final BsonInt64 txnNumber;
    private final BsonDocument lsid;
    private final BsonDateTime wallTime;
    private final SplitEvent splitEvent;
    @BsonExtraElements
    private final BsonDocument extraElements;

    /**
     * Creates a new instance
     *
     * @param operationType the operation type
     * @param resumeToken the resume token
     * @param namespaceDocument the BsonDocument representing the namespace
     * @param namespaceType the namespace type
     * @param destinationNamespaceDocument the BsonDocument representing the destinatation namespace
     * @param fullDocument the full document
     * @param fullDocumentBeforeChange the full document before change
     * @param documentKey a document containing the _id of the changed document
     * @param clusterTime the cluster time at which the change occured
     * @param updateDescription the update description
     * @param txnNumber the transaction number
     * @param lsid the identifier for the session associated with the transaction
     * @param wallTime the wall time of the server at the moment the change occurred
     * @param splitEvent the split event
     * @param extraElements any extra elements that are part of the change stream document but not otherwise mapped to fields
     *
     * @since 4.11
     */
    @BsonCreator
    public ChangeStreamDocument(
            @Nullable @BsonProperty("operationType") final String operationType,
            @BsonProperty("resumeToken") final BsonDocument resumeToken,
            @Nullable @BsonProperty("ns") final BsonDocument namespaceDocument,
            @Nullable @BsonProperty("nsType") final String  namespaceType,
            @Nullable @BsonProperty("to") final BsonDocument destinationNamespaceDocument,
            @Nullable @BsonProperty("fullDocument") final TDocument fullDocument,
            @Nullable @BsonProperty("fullDocumentBeforeChange") final TDocument fullDocumentBeforeChange,
            @Nullable @BsonProperty("documentKey") final BsonDocument documentKey,
            @Nullable @BsonProperty("clusterTime") final BsonTimestamp clusterTime,
            @Nullable @BsonProperty("updateDescription") final UpdateDescription updateDescription,
            @Nullable @BsonProperty("txnNumber") final BsonInt64 txnNumber,
            @Nullable @BsonProperty("lsid") final BsonDocument lsid,
            @Nullable @BsonProperty("wallTime") final BsonDateTime wallTime,
            @Nullable @BsonProperty("splitEvent") final SplitEvent splitEvent,
            @Nullable @BsonProperty final BsonDocument extraElements) {
        this.resumeToken = resumeToken;
        this.namespaceDocument = namespaceDocument;
        this.namespaceTypeString = namespaceType;
        this.namespaceType = namespaceTypeString == null ? null : NamespaceType.fromString(namespaceType);
        this.destinationNamespaceDocument = destinationNamespaceDocument;
        this.fullDocumentBeforeChange = fullDocumentBeforeChange;
        this.documentKey = documentKey;
        this.fullDocument = fullDocument;
        this.clusterTime = clusterTime;
        this.operationTypeString = operationType;
        this.operationType = operationTypeString == null ? null : OperationType.fromString(operationTypeString);
        this.updateDescription = updateDescription;
        this.txnNumber = txnNumber;
        this.lsid = lsid;
        this.wallTime = wallTime;
        this.splitEvent = splitEvent;
        this.extraElements = extraElements;
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
     * <p>
     * The invalidate operation type does include a MongoNamespace in the ChangeStreamDocument response. The
     * dropDatabase operation type includes a MongoNamespace, but does not include a collection name as part
     * of the namespace.
     *
     * @return the namespace. If the namespaceDocument is null or if it is missing either the 'db' or 'coll' keys,
     * then this will return null.
     * @see #getNamespaceType()
     * @see #getNamespaceTypeString()
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
     * Returns the namespace document, derived from the "ns" field in a change stream document.
     * <p>
     * The namespace document is a BsonDocument containing the values associated with a MongoNamespace. The
     * 'db' key refers to the database name and the 'coll' key refers to the collection name.
     *
     * @return the namespaceDocument
     * @since 3.8
     * @see #getNamespaceType()
     * @see #getNamespaceTypeString()
     */
    @BsonProperty("ns")
    @Nullable
    public BsonDocument getNamespaceDocument() {
        return namespaceDocument;
    }

    /**
     * Returns the type of the newly created namespace object as a String, derived from the "nsType" field in a change stream document.
     * <p>
     * This method is useful when using a driver release that has not yet been updated to include a newer namespace type in the
     * {@link NamespaceType} enum.  In that case, {@link #getNamespaceType()} will return {@link NamespaceType#OTHER} and this method can
     * be used to retrieve the actual namespace type as a string value.
     * <p>
     * May return null only if <code>$changeStreamSplitLargeEvent</code> is used.
     *
     * @return the namespace type as a string
     * @since 5.6
     * @mongodb.server.release 8.1
     * @see #getNamespaceType()
     * @see #getNamespaceDocument()
     */
    @Nullable
    public String getNamespaceTypeString() {
        return namespaceTypeString;
    }

    /**
     * Returns the type of the newly created namespace object, derived from the "nsType" field in a change stream document.
     *
     * @return the namespace type.
     * @since 5.6
     * @mongodb.server.release 8.1
     * @see #getNamespaceTypeString()
     * @see #getNamespaceDocument()
     */
    @Nullable
    public NamespaceType getNamespaceType() {
        return namespaceType;
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
     * Returns the fullDocument.
     *
     * <p>
     * Always present for operations of type {@link OperationType#INSERT} and {@link OperationType#REPLACE}. Also present for operations
     * of type {@link OperationType#UPDATE} if the user has specified {@link FullDocument#UPDATE_LOOKUP} for the {@code fullDocument}
     * option when creating the change stream.
     * </p>
     *
     * <p>
     * For operations of type {@link OperationType#INSERT} and {@link OperationType#REPLACE}, the value will contain the document being
     * inserted or the new version of the document that is replacing the existing document, respectively.
     * </p>
     *
     * <p>
     * For operations of type {@link OperationType#UPDATE}, the value will contain a copy of the full version of the document from some
     * point after the update occurred. If the document was deleted since the updated happened, the value may be null.
     * </p>
     *
     * <p>
     * Contains the point-in-time post-image of the modified document if the post-image is available and either
     * {@link FullDocument#REQUIRED} or {@link FullDocument#WHEN_AVAILABLE} was specified for the {@code fullDocument} option when
     * creating the change stream. A post-image is always available for {@link OperationType#INSERT} and {@link OperationType#REPLACE}
     * events.
     * </p>
     *
     * @return the fullDocument
     */
    @Nullable
    public TDocument getFullDocument() {
        return fullDocument;
    }

    /**
     * Returns the fullDocument before change
     *
     * <p>
     * Contains the pre-image of the modified or deleted document if the pre-image is available for the change event and either
     * {@link FullDocumentBeforeChange#REQUIRED} or {@link FullDocumentBeforeChange#WHEN_AVAILABLE} was specified for the
     * {@code fullDocumentBeforeChange} option when creating the change stream. If {@link FullDocumentBeforeChange#WHEN_AVAILABLE} was
     * specified but the pre-image is unavailable, the value will be null.
     * </p>
     *
     * @return the fulDocument before change
     * @since 4.7
     * @mongodb.server.release 6.0
     */
    @Nullable
    public TDocument getFullDocumentBeforeChange() {
        return fullDocumentBeforeChange;
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
     * Returns the operation type as a string.
     * <p>
     * This method is useful when using a driver release that has not yet been updated to include a newer operation type in the
     * {@link OperationType} enum.  In that case, {@link #getOperationType()} will return {@link OperationType#OTHER} and this method can
     * be used to retrieve the actual operation type as a string value.
     * <p>
     * May return null only if <code>$changeStreamSplitLargeEvent</code> is used.
     *
     * @return the operation type as a string
     * @since 4.6
     * @see #getOperationType()
     */
    @Nullable
    public String getOperationTypeString() {
        return operationTypeString;
    }

    /**
     * Returns the operationType.
     * <p>
     * May return null only if <code>$changeStreamSplitLargeEvent</code> is used.
     *
     * @return the operationType
     */
    @Nullable
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
     * The wall time of the server at the moment the change occurred.
     *
     * @return The wall time of the server at the moment the change occurred.
     * @since 4.7
     * @mongodb.server.release 6.0
     */
    @Nullable
    public BsonDateTime getWallTime() {
        return wallTime;
    }

    /**
     * The split event.
     *
     * @return the split event
     * @since 4.11
     * @mongodb.server.release 6.0.9
     */
    @Nullable
    public SplitEvent getSplitEvent() {
        return splitEvent;
    }

    /**
     * Any extra elements that are part of the change stream document but not otherwise mapped to fields.
     *
     * @return Any extra elements that are part of the change stream document but not otherwise mapped to fields.
     * @since 4.7
     */
    @Nullable
    public BsonDocument getExtraElements() {
        return extraElements;
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
        return new ChangeStreamDocumentCodec<>(fullDocumentClass, codecRegistry);
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
        return Objects.equals(resumeToken, that.resumeToken)
                && Objects.equals(namespaceDocument, that.namespaceDocument)
                && Objects.equals(destinationNamespaceDocument, that.destinationNamespaceDocument)
                && Objects.equals(fullDocument, that.fullDocument)
                && Objects.equals(fullDocumentBeforeChange, that.fullDocumentBeforeChange)
                && Objects.equals(documentKey, that.documentKey)
                && Objects.equals(clusterTime, that.clusterTime)
                && Objects.equals(operationTypeString, that.operationTypeString)
                // operationType covered by operationTypeString
                && Objects.equals(updateDescription, that.updateDescription)
                && Objects.equals(txnNumber, that.txnNumber)
                && Objects.equals(lsid, that.lsid)
                && Objects.equals(wallTime, that.wallTime)
                && Objects.equals(splitEvent, that.splitEvent)
                && Objects.equals(extraElements, that.extraElements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                resumeToken,
                namespaceDocument,
                destinationNamespaceDocument,
                fullDocument,
                fullDocumentBeforeChange,
                documentKey,
                clusterTime,
                operationTypeString,
                // operationType covered by operationTypeString
                updateDescription,
                txnNumber,
                lsid,
                wallTime,
                splitEvent,
                extraElements);
    }

    @Override
    public String toString() {
        return "ChangeStreamDocument{"
                + " operationType=" + operationTypeString
                + ", resumeToken=" + resumeToken
                + ", namespace=" + getNamespace()
                + ", destinationNamespace=" + getDestinationNamespace()
                + ", fullDocument=" + fullDocument
                + ", fullDocumentBeforeChange=" + fullDocumentBeforeChange
                + ", documentKey=" + documentKey
                + ", clusterTime=" + clusterTime
                + ", updateDescription=" + updateDescription
                + ", txnNumber=" + txnNumber
                + ", lsid=" + lsid
                + ", splitEvent=" + splitEvent
                + ", wallTime=" + wallTime
                + "}";
    }
}
