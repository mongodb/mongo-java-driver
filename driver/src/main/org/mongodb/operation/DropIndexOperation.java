package org.mongodb.operation;

import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.protocol.CommandProtocol;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerChannelProviderOptions;
import org.mongodb.session.Session;

public class DropIndexOperation extends BaseOperation<CommandResult> {
    private final Codec<Document> commandCodec = new DocumentCodec();
    private final MongoNamespace namespace;
    private final Document dropIndexesCommand;

    public DropIndexOperation(final MongoNamespace namespace, final String indexName, final BufferProvider bufferProvider,
                              final Session session, final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.namespace = namespace;
        this.dropIndexesCommand = new Document("dropIndexes", namespace.getCollectionName()).append("index", indexName);
    }

    @Override
    public CommandResult execute() {
        final ServerChannelProvider provider = getServerChannelProvider();
        try {
            return new CommandProtocol(namespace.getDatabaseName(), dropIndexesCommand, commandCodec, commandCodec, getBufferProvider(),
                                       provider.getServerDescription(), provider.getChannel(), true).execute();
        } catch (MongoCommandFailureException e) {
            return ignoreNamespaceNotFoundExceptions(e);
        }
    }

    //TODO: work out a way to reuse this
    private CommandResult ignoreNamespaceNotFoundExceptions(final MongoCommandFailureException e) {
        if (!e.getCommandResult().getErrorMessage().equals("ns not found")) {
            throw e;
        }
        return e.getCommandResult();
    }

    private ServerChannelProvider getServerChannelProvider() {
        return getSession().createServerChannelProvider(new ServerChannelProviderOptions(false, new PrimaryServerSelector()));
    }
}
