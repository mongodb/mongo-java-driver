package org.mongodb.file;

import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.file.util.ChunkSize;

import spock.lang.Specification;

class MongoFileStoreConfigSpecification extends Specification {

    def "should create config with defaults"() {
        expect:
        def config = MongoFileStoreConfig.builder().build();

        'fileStore' == config.getBucket();
        MongoFileStoreConfig.DEFAULT_CHUNKSIZE.getChunkSize() == config.getChunkSize();
        ReadPreference.primary() == config.getReadPreference();
        WriteConcern.JOURNALED == config.getWriteConcern();
        true == config.isAsyncDeletes();
        true == config.isEnableCompression();
    }

    def "should create config with non-defaults"() {
        expect:
        def config = MongoFileStoreConfig.builder().asyncDeletes(false).bucket('foo')//
                .chunkSize(ChunkSize.huge_4M).enableCompression(false)//
                .readPreference(ReadPreference.SECONDARY_PREFERRED).writeConcern(WriteConcern.FSYNCED)//
                .build();

        'foo' == config.getBucket();
        ChunkSize.huge_4M.getChunkSize() == config.getChunkSize();
        ReadPreference.secondaryPreferred() == config.getReadPreference();
        WriteConcern.FSYNCED == config.getWriteConcern();
        false == config.isAsyncDeletes();
        false == config.isEnableCompression();
    }
}
