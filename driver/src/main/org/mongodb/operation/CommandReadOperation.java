package org.mongodb.operation;

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoFuture;
import org.mongodb.ReadPreference;
import org.mongodb.binding.ReadBinding;
import org.mongodb.session.Session;

import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;

/**
 * An operation that executes an arbitrary command that reads from the server.
 *
 * @since 3.0
 */
public class CommandReadOperation implements AsyncOperation<CommandResult>, ReadOperation<CommandResult> {
    private final Encoder<Document> commandEncoder;
    private final Decoder<Document> commandDecoder;
    private final String database;
    private final Document commandDocument;
    private final ReadPreference readPreference;

    public CommandReadOperation(final String database, final Document command, final ReadPreference readPreference,
                                final Decoder<Document> commandDecoder, final Encoder<Document> commandEncoder) {
        this.database = database;
        this.commandEncoder = commandEncoder;
        this.commandDecoder = commandDecoder;
        this.commandDocument = command;
        this.readPreference = readPreference;
    }

    @Override
    public CommandResult execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, commandDocument, commandEncoder, commandDecoder, binding);
    }

    @Override
    public MongoFuture<CommandResult> executeAsync(final Session session) {
        return executeWrappedCommandProtocolAsync(database, commandDocument, commandEncoder, commandDecoder, readPreference, session);
    }
}