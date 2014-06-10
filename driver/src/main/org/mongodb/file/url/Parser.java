package org.mongodb.file.url;

import java.net.MalformedURLException;
import java.net.URL;

import org.bson.types.ObjectId;
import org.mongodb.file.util.CompressionMediaTypes;

public final class Parser {

    private Parser() {
        // hidden
    }

    public static URL construct(final ObjectId id, final String fileName, final String mediaType, final String compressionFormat,
            final boolean compress) throws MalformedURLException {

        String protocol = MongoFileUrl.PROTOCOL;
        if (compressionFormat != null) {
            protocol += ":" + compressionFormat;
        } else {
            if (compress && CompressionMediaTypes.isCompressable(mediaType)) {
                protocol += ":" + MongoFileUrl.GZ;
            }
        }
        return construct(String.format("%s:%s?%s#%s", protocol, fileName, id.toString(),
                mediaType == null ? "" : mediaType.toString()));
    }

    public static URL construct(final String spec) throws MalformedURLException {

        return new URL(null, spec, new Handler());
    }

}
