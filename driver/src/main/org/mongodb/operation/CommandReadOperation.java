package org.mongodb.operation;

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoFuture;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ReadBinding;

import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * An operation that executes an arbitrary command that reads from the server.
 *
 * @since 3.0
 */
public class CommandReadOperation implements AsyncReadOperation<CommandResult>, ReadOperation<CommandResult> {
    private final Encoder<Document> commandEncoder;
    private final Decoder<Document> commandDecoder;
    private final String database;
    private final Document commandDocument;

    public CommandReadOperation(final String database, final Document command,
                                final Decoder<Document> commandDecoder, final Encoder<Document> commandEncoder) {
        this.database = database;
        this.commandEncoder = commandEncoder;
        this.commandDecoder = commandDecoder;
        this.commandDocument = command;
    }

    @Override
    public CommandResult execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, commandDocument, commandEncoder, commandDecoder, binding);
    }

    @Override
    public MongoFuture<CommandResult> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(database, commandDocument, commandEncoder, commandDecoder, binding);
    }
}