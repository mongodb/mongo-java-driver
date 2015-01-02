package com.mongodb.async.client.gridfs;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

public class GridFSFileCodecProvider implements CodecProvider {

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (clazz == GridFSFile.class) {
            return (Codec<T>) new GridFSFileCodec(registry);
        }
        return null;
    }
}
