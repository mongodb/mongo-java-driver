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

package com.mongodb.internal.connection;

import com.mongodb.MongoInternalException;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.io.ByteBufferBsonInput;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PACKAGE;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.connection.ByteBufBsonHelper.readBsonValue;
import static java.util.Collections.emptyMap;

/**
 * A memory-efficient, read-only {@link BsonDocument} implementation backed by a {@link ByteBuf}.
 *
 * <h2>Overview</h2>
 * <p>This class provides lazy access to BSON document fields without fully deserializing the document
 * into memory. It reads field values directly from the underlying byte buffer on demand, which is
 * particularly useful for large documents where only a few fields need to be accessed.</p>
 *
 * <h2>Data Sources</h2>
 * <p>A {@code ByteBufBsonDocument} can contain data from two sources:</p>
 * <ul>
 *   <li><b>Body fields:</b> Standard BSON document fields stored in {@link #bodyByteBuf}. These are
 *       read lazily using a {@link BsonBinaryReader}.</li>
 *   <li><b>Sequence fields:</b> MongoDB OP_MSG Type 1 payload sequences stored in {@link #sequenceFields}.
 *       These are used when parsing command messages that contain document sequences (e.g., bulk inserts).
 *       Each sequence field appears as an array of documents when accessed.</li>
 * </ul>
 *
 * <h2>OP_MSG Command Message Support</h2>
 * <p>The {@link #createCommandMessage(CompositeByteBuf)} factory method parses MongoDB OP_MSG format,
 * which consists of:</p>
 * <ol>
 *   <li>A body section (Type 0): The main command document</li>
 *   <li>Zero or more document sequence sections (Type 1): Arrays of documents identified by field name</li>
 * </ol>
 * <p>For example, an insert command might have the body containing {@code {insert: "collection", $db: "test"}}
 * and a sequence section with field name "documents" containing the documents to insert.</p>
 *
 * <h2>Resource Management</h2>
 * <p>This class implements {@link Closeable} and manages several types of resources:</p>
 * <ul>
 *   <li><b>ByteBuf instances:</b> The body buffer and any duplicated buffers created during iteration
 *       or value access are tracked in {@link #trackedResources} and released on {@link #close()}.</li>
 *   <li><b>Nested ByteBufBsonDocument/ByteBufBsonArray:</b> When accessing nested documents or arrays,
 *       new {@code ByteBufBsonDocument} or {@link ByteBufBsonArray} instances are created. These are
 *       registered as closeables and closed recursively when the parent is closed.</li>
 *   <li><b>Sequence field documents:</b> Documents within sequence fields are also {@code ByteBufBsonDocument}
 *       instances that are tracked and closed with the parent.</li>
 * </ul>
 *
 * <p><b>Important:</b> Always close this document when done to prevent memory leaks. After closing,
 * any operation will throw {@link IllegalStateException}.</p>
 *
 * <h2>Caching Strategy</h2>
 * <p>The class uses lazy caching to optimize repeated access:</p>
 * <ul>
 *   <li>{@link #cachedDocument}: Once {@link #toBsonDocument()} is called, the fully hydrated document
 *       is cached and all subsequent operations use this cache. At this point, the underlying buffers
 *       are released since they're no longer needed.</li>
 *   <li>{@link #cachedFirstKey}: The first key is cached after the first call to {@link #getFirstKey()}.</li>
 *   <li>Sequence field arrays are cached within {@link SequenceField} after first access.</li>
 * </ul>
 *
 * <h2>Immutability</h2>
 * <p>This class is read-only. All mutation methods ({@link #put}, {@link #remove}, {@link #clear}, etc.)
 * throw {@link UnsupportedOperationException}.</p>
 *
 * <h2>Thread Safety</h2>
 * <p>This class is not thread-safe. Concurrent access from multiple threads requires external synchronization.</p>
 *
 * <h2>Serialization</h2>
 * <p>Java serialization is supported via {@link #writeReplace()}, which converts this document to a
 * regular {@link BsonDocument} before serialization.</p>
 *
 * @see ByteBufBsonArray
 * @see ByteBufBsonHelper
 */
public final class ByteBufBsonDocument extends BsonDocument implements Closeable {
    private static final long serialVersionUID = 2L;

    /**
     * The underlying byte buffer containing the BSON document body.
     * This is the main document data, excluding any OP_MSG sequence sections.
     * Set to null after {@link #releaseResources()} is called.
     */
    private transient ByteBuf bodyByteBuf;

    /**
     * Map of sequence field names to their corresponding {@link SequenceField} instances.
     * These represent OP_MSG Type 1 payload sections. Each sequence field appears as an
     * array when accessed via {@link #get(Object)}.
     * Empty for simple documents not created from OP_MSG.
     */
    private transient Map<String, SequenceField> sequenceFields;

    /**
     * List of resources that need to be closed/released when this document is closed.
     *
     * <p><strong>Memory Management Strategy:</strong></p>
     * <ul>
     *   <li><strong>Always tracked:</strong> The main bodyByteBuf and any nested ByteBufBsonDocument/ByteBufBsonArray
     *       instances returned to callers are permanently tracked until this document is closed or
     *       {@link #toBsonDocument()} caches and releases them.</li>
     *   <li><strong>Temporarily tracked:</strong> Iterator duplicate buffers are tracked during iteration
     *       but automatically removed and released when iteration completes. This prevents memory accumulation
     *       from completed iterations while ensuring cleanup if the parent document is closed mid-iteration.</li>
     *   <li><strong>Not tracked:</strong> Short-lived duplicate buffers used in query methods
     *       (e.g., {@link #findKeyInBody}, {@link #containsKey}) are released immediately in finally blocks
     *       and never added to this list. Temporary nested documents created during value comparison
     *       use separate tracking lists.</li>
     * </ul>
     */
    private final transient List<Closeable> trackedResources;

    /**
     * Cached fully-hydrated BsonDocument. Once populated via {@link #toBsonDocument()},
     * all subsequent read operations use this cache instead of reading from the byte buffer.
     */
    private transient BsonDocument cachedDocument;

    /**
     * Cached first key of the document. Populated on first call to {@link #getFirstKey()}.
     */
    private transient String cachedFirstKey;

    /**
     * Flag indicating whether this document has been closed.
     * Once closed, all operations throw {@link IllegalStateException}.
     */
    private transient boolean closed;


    /**
     * Creates a {@code ByteBufBsonDocument} from an OP_MSG command message.
     *
     * <p>This factory method parses the MongoDB OP_MSG wire protocol format, which consists of:</p>
     * <ol>
     *   <li><b>Body section (Type 0):</b> A single BSON document containing the command</li>
     *   <li><b>Document sequence sections (Type 1):</b> Zero or more sections, each containing
     *       a field identifier and a sequence of BSON documents</li>
     * </ol>
     *
     * <p>The sequence sections are stored in {@link #sequenceFields} and appear as array fields
     * when the document is accessed. For example, an insert command's "documents" sequence
     * will appear as an array when calling {@code get("documents")}.</p>
     *
     * <h3>Wire Format Parsed</h3>
     * <pre>
     * [body document bytes]
     * [section type: 1 byte] [section size: 4 bytes] [identifier: cstring] [document bytes...]
     * ... (more sections)
     * </pre>
     *
     * @param commandMessageByteBuf The composite buffer positioned at the start of the body document.
     *                              Position will be advanced past all parsed sections.
     * @return A new {@code ByteBufBsonDocument} representing the command with any sequence fields.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public static ByteBufBsonDocument createCommandMessage(final CompositeByteBuf commandMessageByteBuf) {
        // Parse body document: read size, create a view of just the body bytes
        int bodyStart = commandMessageByteBuf.position();
        int bodySizeInBytes = commandMessageByteBuf.getInt();
        int bodyEnd = bodyStart + bodySizeInBytes;
        ByteBuf bodyByteBuf = commandMessageByteBuf.duplicate().position(bodyStart).limit(bodyEnd);

        List<Closeable> trackedResources = new ArrayList<>();
        commandMessageByteBuf.position(bodyEnd);

        // Parse any Type 1 (document sequence) sections that follow the body
        Map<String, SequenceField> sequences = new LinkedHashMap<>();
        while (commandMessageByteBuf.hasRemaining()) {
            // Skip section type byte (we only support Type 1 here)
            commandMessageByteBuf.position(commandMessageByteBuf.position() + 1);

            // Read section size and calculate bounds
            int sequenceStart = commandMessageByteBuf.position();
            int sequenceSizeInBytes = commandMessageByteBuf.getInt();
            int sectionEnd = sequenceStart + sequenceSizeInBytes;

            // Read the field identifier (null-terminated string)
            String fieldName = readCString(commandMessageByteBuf);
            assertFalse(fieldName.contains("."));

            // Create a view of just the document sequence bytes (after the identifier)
            ByteBuf sequenceByteBuf = commandMessageByteBuf.duplicate();
            sequenceByteBuf.position(commandMessageByteBuf.position()).limit(sectionEnd);
            sequences.put(fieldName, new SequenceField(sequenceByteBuf, trackedResources));
            commandMessageByteBuf.position(sectionEnd);
        }
        return new ByteBufBsonDocument(bodyByteBuf, trackedResources, sequences);
    }

    /**
     * Creates a simple {@code ByteBufBsonDocument} from a byte buffer containing a single BSON document.
     *
     * <p>Use this constructor for standard BSON documents. For OP_MSG command messages with
     * document sequences, use {@link #createCommandMessage(CompositeByteBuf)} instead.</p>
     *
     * @param byteBuf The buffer containing the BSON document. The buffer should be positioned
     *                at the start of the document and contain the complete document bytes.
     */
    @VisibleForTesting(otherwise = PACKAGE)
    public ByteBufBsonDocument(final ByteBuf byteBuf) {
        this(byteBuf, new ArrayList<>(), new HashMap<>());
    }

    /**
     * Private constructor used by factory methods.
     *
     * @param bodyByteBuf      The buffer containing the body document bytes
     * @param trackedResources Mutable list for tracking resources to close
     * @param sequenceFields   Map of sequence field names to their data (empty for simple documents)
     */
    private ByteBufBsonDocument(final ByteBuf bodyByteBuf, final List<Closeable> trackedResources,
            final Map<String, SequenceField> sequenceFields) {
        this.bodyByteBuf = bodyByteBuf;
        this.trackedResources = trackedResources;
        this.sequenceFields = sequenceFields;
        trackedResources.add(bodyByteBuf::release);
    }

    // ==================== Size and Empty Checks ====================

    @Override
    public int size() {
        ensureOpen();
        if (cachedDocument != null) {
            return cachedDocument.size();
        }
        // Total size = body fields + sequence fields
        return countBodyFields() + sequenceFields.size();
    }

    @Override
    public boolean isEmpty() {
        ensureOpen();
        if (cachedDocument != null) {
            return cachedDocument.isEmpty();
        }
        return !hasBodyFields() && sequenceFields.isEmpty();
    }

    // ==================== Key/Value Lookups ====================

    @Override
    public boolean containsKey(final Object key) {
        ensureOpen();
        if (cachedDocument != null) {
            return cachedDocument.containsKey(key);
        }
        if (key == null) {
            throw new IllegalArgumentException("key can not be null");
        }
        // Check sequence fields first (fast HashMap lookup), then scan body
        if (sequenceFields.containsKey(key)) {
            return true;
        }
        return findKeyInBody((String) key);
    }

    @Override
    public boolean containsValue(final Object value) {
        ensureOpen();
        if (!(value instanceof BsonValue)) {
            return false;
        }

        if (cachedDocument != null) {
            return cachedDocument.containsValue(value);
        }

        // Search body fields first, then sequence fields
        if (findValueInBody((BsonValue) value)) {
            return true;
        }
        for (SequenceField field : sequenceFields.values()) {
            if (field.containsValue(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * <p>For sequence fields (OP_MSG document sequences), returns a {@link BsonArray} containing
     * {@code ByteBufBsonDocument} instances for each document in the sequence.</p>
     */
    @Nullable
    @Override
    public BsonValue get(final Object key) {
        ensureOpen();
        notNull("key", key);

        if (!(key instanceof String)) {
            return null;
        }
        if (cachedDocument != null) {
            return cachedDocument.get(key);
        }

        // Check sequence fields first, then body
        if (sequenceFields.containsKey(key)) {
            return sequenceFields.get(key).asArray();
        }
        return getValueFromBody((String) key);
    }

    @Override
    public String getFirstKey() {
        ensureOpen();
        if (cachedDocument != null) {
            return cachedDocument.getFirstKey();
        }
        if (cachedFirstKey != null) {
            return cachedFirstKey;
        }
        cachedFirstKey = getFirstKeyFromBody();
        return assertNotNull(cachedFirstKey);
    }

    // ==================== Collection Views ====================
    // These return lazy views that iterate over both body and sequence fields

    @Override
    public Set<Entry<String, BsonValue>> entrySet() {
        ensureOpen();
        if (cachedDocument != null) {
            return cachedDocument.entrySet();
        }
        return new AbstractSet<Entry<String, BsonValue>>() {
            @Override
            public Iterator<Entry<String, BsonValue>> iterator() {
                // Combine body entries with sequence entries
                return new CombinedIterator<>(createBodyIterator(IteratorMode.ENTRIES), createSequenceEntryIterator());
            }

            @Override
            public int size() {
                return ByteBufBsonDocument.this.size();
            }
        };
    }

    @Override
    public Collection<BsonValue> values() {
        ensureOpen();
        if (cachedDocument != null) {
            return cachedDocument.values();
        }
        return new AbstractCollection<BsonValue>() {
            @Override
            public Iterator<BsonValue> iterator() {
                return new CombinedIterator<>(createBodyIterator(IteratorMode.VALUES), createSequenceValueIterator());
            }

            @Override
            public int size() {
                return ByteBufBsonDocument.this.size();
            }
        };
    }

    @Override
    public Set<String> keySet() {
        ensureOpen();
        if (cachedDocument != null) {
            return cachedDocument.keySet();
        }
        return new AbstractSet<String>() {
            @Override
            public Iterator<String> iterator() {
                return new CombinedIterator<>(createBodyIterator(IteratorMode.KEYS), sequenceFields.keySet().iterator());
            }

            @Override
            public int size() {
                return ByteBufBsonDocument.this.size();
            }
        };
    }

    // ==================== Conversion Methods ====================

    @Override
    public BsonReader asBsonReader() {
        ensureOpen();
        // Must hydrate first since we need to include sequence fields
        return toBsonDocument().asBsonReader();
    }

    /**
     * Converts this document to a regular {@link BsonDocument}, fully deserializing all data.
     *
     * <p>After this method is called:</p>
     * <ul>
     *   <li>The result is cached for future calls</li>
     *   <li>All underlying byte buffers are released</li>
     *   <li>Sequence field documents are hydrated to regular {@code BsonDocument} instances</li>
     *   <li>All subsequent read operations use the cached document</li>
     * </ul>
     *
     * @return A fully materialized {@link BsonDocument} containing all fields
     */
    @Override
    public BsonDocument toBsonDocument() {
        ensureOpen();
        if (cachedDocument == null) {
            ByteBuf dup = bodyByteBuf.duplicate();
            try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(dup))) {
                // Decode body document
                BsonDocument doc = new BsonDocumentCodec().decode(reader, DecoderContext.builder().build());
                // Add hydrated sequence fields
                for (Map.Entry<String, SequenceField> entry : sequenceFields.entrySet()) {
                    doc.put(entry.getKey(), entry.getValue().toHydratedArray());
                }
                cachedDocument = doc;
                // Release buffers since we no longer need them
                releaseResources();
            } finally {
                dup.release();
            }
        }
        return cachedDocument;
    }

    @Override
    public String toJson() {
        return toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
    }

    @Override
    public String toJson(final JsonWriterSettings settings) {
        ensureOpen();
        return toBsonDocument().toJson(settings);
    }

    @Override
    public String toString() {
        ensureOpen();
        return toBsonDocument().toString();
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    public BsonDocument clone() {
        ensureOpen();
        return toBsonDocument().clone();
    }

    @SuppressWarnings("EqualsDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object o) {
        ensureOpen();
        return toBsonDocument().equals(o);
    }

    @Override
    public int hashCode() {
        ensureOpen();
        return toBsonDocument().hashCode();
    }

    // ==================== Resource Management ====================

    /**
     * Releases all resources held by this document.
     *
     * <p>This includes:</p>
     * <ul>
     *   <li>Releasing all tracked {@link ByteBuf} instances</li>
     *   <li>Closing all nested {@code ByteBufBsonDocument} and {@link ByteBufBsonArray} instances</li>
     *   <li>Clearing internal references</li>
     * </ul>
     *
     * <p>After calling this method, any operation on this document will throw
     * {@link IllegalStateException}. This method is idempotent.</p>
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            releaseResources();
        }
    }

    // ==================== Mutation Methods (Unsupported) ====================

    @Override
    public BsonValue put(final String key, final BsonValue value) {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    @Override
    public BsonDocument append(final String key, final BsonValue value) {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    @Override
    public void putAll(final Map<? extends String, ? extends BsonValue> m) {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    @Override
    public BsonValue remove(final Object key) {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("ByteBufBsonDocument instances are immutable");
    }

    // ==================== Private Body Field Operations ====================
    // These methods read from bodyByteBuf using a temporary duplicate buffer

    /**
     * Searches the body for a field with the given key.
     * Uses a duplicated buffer to avoid modifying the original position.
     */
    private boolean findKeyInBody(final String key) {
        ByteBuf dup = bodyByteBuf.duplicate();
        try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(dup))) {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (reader.readName().equals(key)) {
                    return true;
                }
                reader.skipValue();
            }
            return false;
        } finally {
            dup.release();
        }
    }

    /**
     * Searches the body for a field with the given value.
     * Creates ByteBufBsonDocument/ByteBufBsonArray for nested structures during comparison or vanilla BsonValues.
     * Uses temporary tracking list to avoid polluting the main trackedResources with short-lived objects.
     */
    private boolean findValueInBody(final BsonValue targetValue) {
        ByteBuf dup = bodyByteBuf.duplicate();
        List<Closeable> tempTrackedResources = new ArrayList<>();
        try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(dup))) {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                reader.skipName();
                if (readBsonValue(dup, reader, tempTrackedResources).equals(targetValue)) {
                    return true;
                }
            }
            return false;
        } finally {
            // Release temporary resources created during comparison
            for (Closeable resource : tempTrackedResources) {
                try {
                    resource.close();
                } catch (Exception e) {
                    // Continue closing other resources
                }
            }
            dup.release();
        }
    }

    /**
     * Retrieves a value from the body by key.
     * Returns null if the key is not found in the body.
     */
    @Nullable
    private BsonValue getValueFromBody(final String key) {
        ByteBuf dup = bodyByteBuf.duplicate();
        try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(dup))) {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (reader.readName().equals(key)) {
                    return readBsonValue(dup, reader, trackedResources);
                }
                reader.skipValue();
            }
            return null;
        } finally {
            dup.release();
        }
    }

    /**
     * Gets the first key from the body, or from sequence fields if body is empty.
     * Throws NoSuchElementException if the document is completely empty.
     */
    private String getFirstKeyFromBody() {
        ByteBuf dup = bodyByteBuf.duplicate();
        try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(dup))) {
            reader.readStartDocument();
            if (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                return reader.readName();
            }
            // Body is empty, try sequence fields
            if (!sequenceFields.isEmpty()) {
                return sequenceFields.keySet().iterator().next();
            }
            throw new NoSuchElementException();
        } finally {
            dup.release();
        }
    }

    /**
     * Checks if the body contains at least one field.
     */
    private boolean hasBodyFields() {
        ByteBuf dup = bodyByteBuf.duplicate();
        try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(dup))) {
            reader.readStartDocument();
            return reader.readBsonType() != BsonType.END_OF_DOCUMENT;
        } finally {
            dup.release();
        }
    }

    /**
     * Counts the number of fields in the body document.
     */
    private int countBodyFields() {
        ByteBuf dup = bodyByteBuf.duplicate();
        try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(dup))) {
            reader.readStartDocument();
            int count = 0;
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                count++;
                reader.skipName();
                reader.skipValue();
            }
            return count;
        } finally {
            dup.release();
        }
    }

    // ==================== Iterator Support ====================

    /**
     * Mode for the body iterator, determining what type of elements it produces.
     */
    private enum IteratorMode { ENTRIES, KEYS, VALUES }

    /**
     * Creates an iterator over the body document fields.
     *
     * <p>The iterator creates a duplicated ByteBuf that is temporarily tracked for safety.
     * When iteration completes normally, the buffer is released immediately and removed from tracking.
     * This prevents accumulation of finished iterator buffers while ensuring cleanup if the parent
     * document is closed before iteration completes.</p>
     *
     * @param mode Determines whether to return entries, keys, or values
     * @return An iterator of the appropriate type
     */
    @SuppressWarnings("unchecked")
    private <T> Iterator<T> createBodyIterator(final IteratorMode mode) {
        return new Iterator<T>() {
            private ByteBuf duplicatedByteBuf;
            private BsonBinaryReader reader;
            private Closeable resourceHandle;
            private boolean started;
            private boolean finished;

            {
                // Create duplicate buffer for iteration and track it temporarily
                duplicatedByteBuf = bodyByteBuf.duplicate();
                resourceHandle = () -> {
                    if (duplicatedByteBuf != null) {
                        try {
                            if (reader != null) {
                                reader.close();
                            }
                        } catch (Exception e) {
                            // Ignore
                        }
                        duplicatedByteBuf.release();
                        duplicatedByteBuf = null;
                        reader = null;
                    }
                };
                trackedResources.add(resourceHandle);
                reader = new BsonBinaryReader(new ByteBufferBsonInput(duplicatedByteBuf));
            }

            @Override
            public boolean hasNext() {
                if (finished) {
                    return false;
                }
                if (!started) {
                    reader.readStartDocument();
                    reader.readBsonType();
                    started = true;
                }
                boolean hasNext = reader.getCurrentBsonType() != BsonType.END_OF_DOCUMENT;
                if (!hasNext) {
                    cleanup();
                }
                return hasNext;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                String key = reader.readName();
                BsonValue value = readBsonValue(duplicatedByteBuf, reader, trackedResources);
                reader.readBsonType();

                switch (mode) {
                    case ENTRIES:
                        return (T) new AbstractMap.SimpleImmutableEntry<>(key, value);
                    case KEYS:
                        return (T) key;
                    case VALUES:
                        return (T) value;
                    default:
                        throw new IllegalStateException("Unknown iterator mode: " + mode);
                }
            }

            private void cleanup() {
                if (!finished) {
                    finished = true;
                    // Remove from tracked resources since we're cleaning up immediately
                    trackedResources.remove(resourceHandle);
                    try {
                        resourceHandle.close();
                    } catch (Exception e) {
                        // Ignore
                    }
                }
            }
        };
    }

    /**
     * Creates an iterator over sequence fields as map entries.
     * Each entry contains the field name and its array value.
     */
    private Iterator<Entry<String, BsonValue>> createSequenceEntryIterator() {
        Iterator<Map.Entry<String, SequenceField>> iter = sequenceFields.entrySet().iterator();
        return new Iterator<Entry<String, BsonValue>>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Entry<String, BsonValue> next() {
                Map.Entry<String, SequenceField> entry = iter.next();
                return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue().asArray());
            }
        };
    }

    /**
     * Creates an iterator over sequence field values (arrays).
     */
    private Iterator<BsonValue> createSequenceValueIterator() {
        Iterator<SequenceField> iter = sequenceFields.values().iterator();
        return new Iterator<BsonValue>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public BsonValue next() {
                return iter.next().asArray();
            }
        };
    }

    // ==================== Resource Management Helpers ====================

    /**
     * Releases all tracked resources and clears internal state.
     *
     * <p>Called by {@link #close()} and after {@link #toBsonDocument()} caches the result.
     * Resources include ByteBuf instances and nested ByteBufBsonDocument/ByteBufBsonArray.</p>
     */
    private void releaseResources() {
        for (Closeable resource : trackedResources) {
            try {
                resource.close();
            } catch (Exception e) {
                // Log and continue closing other resources
            }
        }

        assertTrue(bodyByteBuf == null || bodyByteBuf.getReferenceCount() == 0, "Failed to release all `bodyByteBuf` resources");
        assertTrue(sequenceFields.values().stream().allMatch(b -> b.sequenceByteBuf.getReferenceCount() == 0),
                "Failed to release all `sequenceField` resources");

        trackedResources.clear();
        sequenceFields = emptyMap();
        bodyByteBuf = null;
        cachedFirstKey = null;
    }

    /**
     * Throws IllegalStateException if this document has been closed.
     */
    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("The BsonDocument resources have been released.");
        }
    }

    // ==================== Utility Methods ====================

    /**
     * Reads a null-terminated C-string from the buffer.
     * Used for parsing OP_MSG sequence identifiers.
     */
    private static String readCString(final ByteBuf byteBuf) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte b = byteBuf.get();
        while (b != 0) {
            bytes.write(b);
            b = byteBuf.get();
        }
        try {
            return bytes.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new MongoInternalException("Unexpected exception", e);
        }
    }

    /**
     * Serialization support: converts to a regular BsonDocument before serialization.
     */
    private Object writeReplace() {
        ensureOpen();
        return toBsonDocument();
    }

    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }

    // ==================== Inner Classes ====================

    /**
     * Represents an OP_MSG Type 1 document sequence section.
     *
     * <p>A sequence field contains a contiguous series of BSON documents in the buffer.
     * When accessed via {@link #asArray()}, it returns a {@link BsonArray} containing
     * {@link ByteBufBsonDocument} instances for each document.</p>
     *
     * <p>The documents are lazily parsed on first access and cached for subsequent calls.</p>
     */
    private static final class SequenceField {
        /** Buffer containing the sequence of BSON documents */
        private final ByteBuf sequenceByteBuf;

        /** Reference to parent's tracked resources for registering created documents */
        private final List<Closeable> trackedResources;

        /** Cached list of parsed documents, populated on first access */
        private List<BsonDocument> documents;

        SequenceField(final ByteBuf sequenceByteBuf, final List<Closeable> trackedResources) {
            this.sequenceByteBuf = sequenceByteBuf;
            this.trackedResources = trackedResources;
            trackedResources.add(sequenceByteBuf::release);
        }

        /**
         * Returns this sequence as a BsonArray of ByteBufBsonDocument instances.
         *
         * <p>On first call, parses the buffer to create ByteBufBsonDocument for each
         * document and registers them with the parent's tracked resources.</p>
         *
         * @return A BsonArray containing the sequence documents
         */
        BsonValue asArray() {
            if (documents == null) {
                documents = new ArrayList<>();
                ByteBuf dup = sequenceByteBuf.duplicate();
                try {
                    while (dup.hasRemaining()) {
                        // Read document size to determine bounds
                        int docStart = dup.position();
                        int docSize = dup.getInt();
                        int docEnd = docStart + docSize;

                        // Create a view of just this document's bytes
                        ByteBuf docBuf = sequenceByteBuf.duplicate().position(docStart).limit(docEnd);
                        ByteBufBsonDocument doc = new ByteBufBsonDocument(docBuf);
                        // Track for cleanup when parent is closed
                        trackedResources.add(doc);
                        documents.add(doc);
                        dup.position(docEnd);
                    }
                } finally {
                    dup.release();
                }
            }
            // Return a new array each time to prevent external modification of cached list
            return new BsonArray(new ArrayList<>(documents));
        }

        /**
         * Checks if this sequence contains the given value.
         */
        boolean containsValue(final Object value) {
            return value instanceof BsonValue && asArray().asArray().contains(value);
        }

        /**
         * Converts this sequence to a BsonArray of regular BsonDocument instances.
         *
         * <p>Used by {@link ByteBufBsonDocument#toBsonDocument()} to fully hydrate the document.
         * Unlike {@link #asArray()}, this creates regular BsonDocument instances, not
         * ByteBufBsonDocument wrappers.</p>
         *
         * @return A BsonArray containing fully deserialized BsonDocument instances
         */
        BsonArray toHydratedArray() {
            ByteBuf dup = sequenceByteBuf.duplicate();
            try {
                List<BsonValue> hydratedDocs = new ArrayList<>();
                while (dup.hasRemaining()) {
                    int docStart = dup.position();
                    int docSize = dup.getInt();
                    int docEnd = docStart + docSize;
                    ByteBuf docBuf = dup.duplicate().position(docStart).limit(docEnd);
                    try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(docBuf))) {
                        hydratedDocs.add(new BsonDocumentCodec().decode(reader, DecoderContext.builder().build()));
                    } finally {
                        docBuf.release();
                    }
                    dup.position(docEnd);
                }
                return new BsonArray(hydratedDocs);
            } finally {
                dup.release();
            }
        }
    }

    /**
     * An iterator that combines two iterators sequentially.
     *
     * <p>Used to merge body field iteration with sequence field iteration,
     * presenting a unified view of all document fields.</p>
     *
     * @param <T> The type of elements returned by the iterator
     */
    private static final class CombinedIterator<T> implements Iterator<T> {
        private final Iterator<? extends T> primary;
        private final Iterator<? extends T> secondary;

        CombinedIterator(final Iterator<? extends T> primary, final Iterator<? extends T> secondary) {
            this.primary = primary;
            this.secondary = secondary;
        }

        @Override
        public boolean hasNext() {
            return primary.hasNext() || secondary.hasNext();
        }

        @Override
        public T next() {
            if (primary.hasNext()) {
                return primary.next();
            }
            if (secondary.hasNext()) {
                return secondary.next();
            }
            throw new NoSuchElementException();
        }
    }
}
