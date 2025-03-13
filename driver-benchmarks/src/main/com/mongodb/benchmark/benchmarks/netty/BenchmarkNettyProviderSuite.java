/*
 * Copyright 2016-present MongoDB, Inc.
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
 *
 */

package com.mongodb.benchmark.benchmarks.netty;

import com.mongodb.MongoClientSettings;
import com.mongodb.benchmark.benchmarks.BenchmarkSuite;
import com.mongodb.benchmark.benchmarks.FindManyBenchmark;
import com.mongodb.benchmark.benchmarks.FindOneBenchmark;
import com.mongodb.benchmark.benchmarks.GridFSDownloadBenchmark;
import com.mongodb.benchmark.benchmarks.GridFSMultiFileDownloadBenchmark;
import com.mongodb.benchmark.benchmarks.GridFSMultiFileUploadBenchmark;
import com.mongodb.benchmark.benchmarks.GridFSUploadBenchmark;
import com.mongodb.benchmark.benchmarks.InsertManyBenchmark;
import com.mongodb.benchmark.benchmarks.InsertOneBenchmark;
import com.mongodb.benchmark.benchmarks.MultiFileExportBenchmark;
import com.mongodb.benchmark.benchmarks.MultiFileImportBenchmark;
import com.mongodb.benchmark.benchmarks.RunCommandBenchmark;
import com.mongodb.benchmark.framework.BenchmarkResultWriter;
import com.mongodb.connection.NettyTransportSettings;
import io.netty.buffer.PooledByteBufAllocator;
import org.bson.Document;

import java.util.List;

import static java.util.Collections.singletonList;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BenchmarkNettyProviderSuite extends BenchmarkSuite {

    public static final List<String> TAGS = singletonList("Netty");
    public static final MongoClientSettings MONGO_CLIENT_SETTINGS = MongoClientSettings.builder()
            .transportSettings(NettyTransportSettings.nettyBuilder()
                    .allocator(PooledByteBufAllocator.DEFAULT)
                    .build())
            .build();

    public static void main(String[] args) throws Exception {
        runBenchmarks();

        for (BenchmarkResultWriter writer : WRITERS) {
            writer.close();
        }
    }

    private static void runBenchmarks()
            throws Exception {
        runBenchmark(new RunCommandBenchmark<>(TAGS, DOCUMENT_CODEC)
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new FindOneBenchmark<Document>(TAGS, "single_and_multi_document/tweet.json",
                DOCUMENT_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new InsertOneBenchmark<Document>(TAGS, "Small", "./single_and_multi_document/small_doc.json", 10000,
                DOCUMENT_CLASS, ID_REMOVER).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new InsertOneBenchmark<Document>(TAGS, "Large", "./single_and_multi_document/large_doc.json", 10,
                DOCUMENT_CLASS, ID_REMOVER).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new FindManyBenchmark<Document>(TAGS, "single_and_multi_document/tweet.json",
                DOCUMENT_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new InsertManyBenchmark<Document>(TAGS, "Small", "./single_and_multi_document/small_doc.json", 10000,
                DOCUMENT_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new InsertManyBenchmark<Document>(TAGS, "Large", "./single_and_multi_document/large_doc.json", 10,
                DOCUMENT_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new GridFSUploadBenchmark(TAGS, "single_and_multi_document/gridfs_large.bin")
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new GridFSDownloadBenchmark(TAGS, "single_and_multi_document/gridfs_large.bin")
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new MultiFileImportBenchmark(TAGS)
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new MultiFileExportBenchmark(TAGS)
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new GridFSMultiFileUploadBenchmark(TAGS)
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new GridFSMultiFileDownloadBenchmark(TAGS)
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
    }
}
