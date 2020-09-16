package org.bson.codecs.pojo;

import org.bson.codecs.Codec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.entities.SimpleModel;
import org.bson.codecs.pojo.entities.records.BsonCreatorRecord;
import org.bson.codecs.pojo.entities.records.SimpleRecord;
import org.junit.Test;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RecordCodecProviderTest {
    @Test
    public void testClassNotFound() {
        RecordCodecProvider provider = RecordCodecProvider.builder().build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<SimpleRecord> codec = provider.get(SimpleRecord.class, registry);
        assertNull(codec);
    }

    @Test
    public void testAutomatic() {
        RecordCodecProvider provider = RecordCodecProvider.builder().automatic(true).build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<SimpleRecord> codec = provider.get(SimpleRecord.class, registry);
        assertNotNull(codec);
    }

    @Test
    public void testAutomaticNotRecord() {
        RecordCodecProvider provider = RecordCodecProvider.builder().automatic(true).build();
        CodecRegistry registry = fromProviders(provider);
        Codec<SimpleModel> codec = provider.get(SimpleModel.class, registry);
        assertNull(codec);
    }

    @Test
    public void testAutomaticInvalidRecord() {
        RecordCodecProvider provider = RecordCodecProvider.builder().automatic(true).build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<BsonCreatorRecord> codec = provider.get(BsonCreatorRecord.class, registry);
        assertNull(codec);
    }

    @Test
    public void testRegisterClass() {
        RecordCodecProvider provider = RecordCodecProvider.builder().register(SimpleRecord.class).build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<SimpleRecord> codec = provider.get(SimpleRecord.class, registry);
        assertNotNull(codec);
    }

    @Test
    public void testRegisterClassModel() {
        ClassModel<SimpleRecord> simpleRecordClassModel = ClassModel.builder(SimpleRecord.class).build();
        RecordCodecProvider provider = RecordCodecProvider.builder().register(simpleRecordClassModel).build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<SimpleRecord> codec = provider.get(SimpleRecord.class, registry);
        assertNotNull(codec);
    }

    @Test
    public void testRegisterPackageName() {
        RecordCodecProvider provider = RecordCodecProvider.builder().register("org.bson.codecs.pojo.entities.records").build();
        CodecRegistry registry = fromProviders(provider, new ValueCodecProvider());
        Codec<SimpleRecord> codec = provider.get(SimpleRecord.class, registry);
        assertNotNull(codec);
    }
}
