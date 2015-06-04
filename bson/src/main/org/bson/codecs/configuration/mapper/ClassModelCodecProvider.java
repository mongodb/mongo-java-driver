package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.TypeResolver;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

public class ClassModelCodecProvider implements CodecProvider {
    private final TypeResolver resolver = new TypeResolver();
    private final Map<Class, ClassModelCodec> codecs = new HashMap<Class, ClassModelCodec>();
    private CodecRegistry registry;

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return codecs.get(clazz);
    }

    public boolean register(final Class clazz) {
        ClassModelCodec codec = codecs.get(clazz);
        if (codec == null) {
            codec = new ClassModelCodec(new ClassModel(registry, resolver, clazz));
            codecs.put(clazz, codec);
        }

        return codec != null;
    }

    public void setRegistry(final CodecRegistry registry) {
        this.registry = registry;
    }
}
