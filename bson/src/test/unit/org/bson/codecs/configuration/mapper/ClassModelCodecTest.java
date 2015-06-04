package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.TypeResolver;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClassModelCodecTest {

    private CodecRegistry registry;
    private ClassModelCodecProvider codecProvider;

    @Test
    public void resolveEntityTypes() {
        final ClassModel model = new ClassModel(getCodecRegistry(), new TypeResolver(), Entity.class);
        assertEquals("Should find 3 fields", 3, model.getFields().size());
    }

    private CodecRegistry getCodecRegistry() {
        if (registry == null) {

            codecProvider = new ClassModelCodecProvider();
            registry = CodecRegistries.fromProviders(codecProvider, new ValueCodecProvider());
            codecProvider.setRegistry(registry);
        }
        return registry;
    }

    @Test
    public void testProvider() {
        final CodecRegistry codecRegistry = getCodecRegistry();
        codecProvider.register(Entity.class);

        Assert.assertTrue(codecRegistry.get(Entity.class) instanceof ClassModelCodec);
    }

    @Test
    public void testRoundTrip() {
        Entity entity = new Entity(800L, 12, "James Bond");

        final CodecRegistry codecRegistry = getCodecRegistry();
        codecProvider.register(Entity.class);

        final BsonDocument document = new BsonDocument();
        final BsonDocumentWriter writer = new BsonDocumentWriter(document);
        final Codec<Entity> codec = codecRegistry.get(Entity.class);
        codec.encode(writer, entity, EncoderContext.builder().build());
        final Entity decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        Assert.assertEquals(entity, decoded);
    }

    private static class Fooble<T> {
        private T value;
    }

    private static class Entity {
        private String name;
        private Integer faves;
        private Long age;

        public Entity() {
        }

        public Entity(final Long age, final int faves, final String name) {
            this.age = age;
            this.faves = faves;
            this.name = name;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + faves;
            result = 31 * result + (age != null ? age.hashCode() : 0);
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Entity entity = (Entity) o;

            if (faves != entity.faves) {
                return false;
            }
            if (name != null ? !name.equals(entity.name) : entity.name != null) {
                return false;
            }
            if (age != null ? !age.equals(entity.age) : entity.age != null) {
                return false;
            }

            return true;
        }
    }
}