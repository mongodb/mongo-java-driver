/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb.operation;

import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.ReadPreference;
import org.mongodb.connection.Connection;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

final class OperationHelper {

    static MongoFuture<ServerDescriptionConnectionPair> getConnectionAsync(final Session session,
                                                                           final ServerConnectionProviderOptions
                                                                               serverConnectionProviderOptions) {
        final SingleResultFuture<ServerDescriptionConnectionPair> retVal = new SingleResultFuture<ServerDescriptionConnectionPair>();
        session.createServerConnectionProviderAsync(serverConnectionProviderOptions)
               .register(new SingleResultCallback<ServerConnectionProvider>() {
                   @Override
                   public void onResult(final ServerConnectionProvider provider, final MongoException e) {
                       if (e != null) {
                           retVal.init(null, e);
                       } else {
                           provider.getConnectionAsync().register(new SingleResultCallback<Connection>() {
                               @Override
                               public void onResult(final Connection connection, final MongoException e) {
                                   if (e != null) {
                                       retVal.init(null, e);
                                   } else {
                                       retVal.init(new ServerDescriptionConnectionPair(provider.getServerDescription(), connection), null);
                                   }
                               }
                           });
                       }
                   }
               });
        return retVal;
    }

    private OperationHelper() {
    }

    /**
     * Use this method to get a ServerConnectionProvider that doesn't rely on specified read preferences.  Used by Operations like commands
     * which always run against the primary.
     *
     * @return a ServerConnectionProvider initialised with a PrimaryServerSelector
     * @param session
     */
    static ServerConnectionProvider getPrimaryServerConnectionProvider(final Session session) {
        return session.createServerConnectionProvider(new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
    }

    /**
     * Use this method to get a provider that works based upon a given readPreference.  According to the implementation of
     * ReadPreferenceServerSelector, if readPreference is null, primary will be used by default.
     *
     * @param readPreference the requested ReadPreference for this operation
     * @param session
     * @return a ServerConnectionProvider initialised with a ReadPreferenceServerSelector
     */
    static ServerConnectionProvider getConnectionProvider(final ReadPreference readPreference, final Session session) {
        ReadPreferenceServerSelector serverSelector = new ReadPreferenceServerSelector(readPreference);
        return session.createServerConnectionProvider(new ServerConnectionProviderOptions(false, serverSelector));
    }
}
