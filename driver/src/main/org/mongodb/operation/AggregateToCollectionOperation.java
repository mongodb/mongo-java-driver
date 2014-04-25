package org.mongodb.operation;

import org.mongodb.AggregationOptions;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.binding.WriteBinding;
import org.mongodb.codecs.DocumentCodec;

import java.util.List;

import static org.mongodb.operation.AggregateHelper.asCommandDocument;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.VoidTransformer;

/**
 * An operation that executes an aggregation that writes its results to a collection (which is what makes this a write operation rather
 * than a read operation).
 *
 * @since 3.0
 */
public class AggregateToCollectionOperation implements WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final List<Document> pipeline;
    private final Encoder<Document> encoder;
    private final AggregationOptions options;

    /**
     * Construct a new instance
     * @param namespace the namespace to aggregate from
     * @param pipeline the aggregate pipeline
     * @param options the aggregation options
     */
    public AggregateToCollectionOperation(final MongoNamespace namespace, final List<Document> pipeline, final Encoder<Document> encoder,
                                           final AggregationOptions options) {
        this.namespace = namespace;
        this.pipeline = pipeline;
        this.encoder = encoder;
        this.options = options;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Void execute(final WriteBinding binding) {
        executeWrappedCommandProtocol(namespace, asCommandDocument(namespace, pipeline, options),
                                      encoder, new DocumentCodec(), binding, new VoidTransformer<CommandResult>());

        return null;
    }
}
