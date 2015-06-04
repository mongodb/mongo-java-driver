package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.TypeResolver;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashSet;
import java.util.Set;

public class ClassModelCodecProvider implements CodecProvider {
    private final TypeResolver resolver = new TypeResolver();
    private final Set<Class> registered;

    public ClassModelCodecProvider(final Set<Class> registered) {
        this.registered = registered;
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        Codec<T> codec = null;
        if(registered.contains(clazz)) {
            codec = new ClassModelCodec(new ClassModel(registry, resolver, clazz));
        }
        return codec;
    }

    public static ProviderBuilder builder() {
        return new ProviderBuilder();
    }

    public static class ProviderBuilder {
        private final Set<Class> registered = new HashSet<Class>();
        
        public ProviderBuilder register(final Class clazz) {
            registered.add(clazz);
            return this;
        }
        
        public ClassModelCodecProvider build() {
            return new ClassModelCodecProvider(registered);
        }
    }
}
