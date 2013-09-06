/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import static org.mongodb.assertions.Assertions.notNull;

public class SocketStreamFactory implements StreamFactory {
    private final SocketSettings settings;
    private final SSLSettings sslSettings;
    private final SocketFactory socketFactory;

    public SocketStreamFactory(final SocketSettings settings, final SSLSettings sslSettings) {
        this.settings = notNull("settings", settings);
        this.sslSettings = notNull("sslSettings", sslSettings);
        this.socketFactory = null;
    }

    public SocketStreamFactory(final SocketSettings settings, final SocketFactory socketFactory) {
        this.settings = notNull("settings", settings);
        this.sslSettings = null;
        this.socketFactory = notNull("socketFactory", socketFactory);
    }

    @Override
    public Stream create(final ServerAddress serverAddress) {
        Stream stream;
        if (socketFactory != null) {
            stream = new SocketStream(serverAddress, settings, socketFactory);
        }
        else if (sslSettings.isEnabled()) {
            stream = new SocketStream(serverAddress, settings, SSLSocketFactory.getDefault());
        }
        else if (System.getProperty("org.mongodb.useSocket", "false").equals("true")) {
            stream = new SocketStream(serverAddress, settings, SocketFactory.getDefault());
        }
        else {
            stream = new SocketChannelStream(serverAddress, settings);
        }

        return stream;
    }
}
