package org.mongodb.file.url;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;


/**
 * A URL format to specifying the need info to store email attachments in MongoDB
 * 
 * The format is as follows :
 * 
 * mongofile:<mime-type>:<mongo id>:<filename.extension>
 * 
 * and is mapped to the following URL fields
 * 
 * protocol:ref:query:path
 * 
 * @author dbusch
 * 
 */
public class Handler extends URLStreamHandler {

    @Override
    protected URLConnection openConnection(final URL u)
            throws IOException {

        throw new UnsupportedOperationException("oppenConnection is currently not supported");
    }

    @Override
    protected void parseURL(final URL u, final String spec, final int start, final int limit) {

        // get what we are working with
        String temp = spec.substring(start, limit);

        // any compression, stored in the host field
        String host = u.getHost();
        int index = temp.indexOf(':');
        if (index != -1) {
            host = temp.substring(0, index);
            temp = temp.substring(index + 1);
        }

        // get the filePath and the document id
        int queryPos = temp.indexOf('?');
        String path = temp.substring(0, queryPos);
        String id = temp.substring(queryPos + 1);

        setURL(u, u.getProtocol(), host, u.getPort(), u.getAuthority(), u.getUserInfo(), path, id, u.getRef());
    }

    @Override
    protected String toExternalForm(final URL u) {

        StringBuilder result = new StringBuilder(100);
        result.append(u.getProtocol());
        result.append(":");
        if (u.getHost() != null) {
            result.append(u.getHost());
            result.append(":");
        }
        if (u.getPath() != null) {
            result.append(u.getPath());
        }
        if (u.getQuery() != null) {
            result.append('?');
            result.append(u.getQuery());
        }
        if (u.getRef() != null) {
            result.append("#");
            result.append(u.getRef());
        }
        return result.toString();
    }

}
