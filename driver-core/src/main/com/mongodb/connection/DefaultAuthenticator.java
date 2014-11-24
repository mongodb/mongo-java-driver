package com.mongodb.connection;

import com.mongodb.MongoCredential;
import com.mongodb.async.SingleResultCallback;

import static com.mongodb.assertions.Assertions.isTrueArgument;

class DefaultAuthenticator extends Authenticator {
    public DefaultAuthenticator(final MongoCredential credential) {
        super(credential);
        isTrueArgument("unspecified authentication mechanism", credential.getAuthenticationMechanism() == null);
    }

    @Override
    void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        createAuthenticator(connectionDescription).authenticate(connection, connectionDescription);
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        createAuthenticator(connectionDescription).authenticateAsync(connection, connectionDescription, callback);
    }

    Authenticator createAuthenticator(final ConnectionDescription connectionDescription) {
        if (connectionDescription.getServerVersion().compareTo(new ServerVersion(2, 7)) >= 0) {
            return new ScramSha1Authenticator(getCredential());
        } else {
            return new NativeAuthenticator(getCredential());
        }
    }
}
