package org.mongodb.file.url;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class MongoFileURLStreamHandlerFactory implements URLStreamHandlerFactory {

    @Override
    public URLStreamHandler createURLStreamHandler(final String protocol) {

        if (protocol.equals(MongoFileUrl.PROTOCOL)) {
            return new Handler();
        }
        return null;
    }

}
