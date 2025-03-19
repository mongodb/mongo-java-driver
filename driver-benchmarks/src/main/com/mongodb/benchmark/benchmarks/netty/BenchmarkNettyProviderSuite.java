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

@SuppressWarnings({"rawtypes", "unchecked"})
public class BenchmarkNettyProviderSuite extends BenchmarkSuite {

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
        runBenchmark(new RunCommandBenchmark<>(DOCUMENT_CODEC)
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new FindOneBenchmark<Document>("single_and_multi_document/tweet.json",
                DOCUMENT_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new InsertOneBenchmark<Document>("Small", "./single_and_multi_document/small_doc.json", 10000,
                DOCUMENT_CLASS, ID_REMOVER).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new InsertOneBenchmark<Document>("Large", "./single_and_multi_document/large_doc.json", 10,
                DOCUMENT_CLASS, ID_REMOVER).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new FindManyBenchmark<Document>("single_and_multi_document/tweet.json",
                DOCUMENT_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new InsertManyBenchmark<Document>("Small", "./single_and_multi_document/small_doc.json", 10000,
                DOCUMENT_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new InsertManyBenchmark<Document>("Large", "./single_and_multi_document/large_doc.json", 10,
                DOCUMENT_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new GridFSUploadBenchmark("single_and_multi_document/gridfs_large.bin")
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new GridFSDownloadBenchmark("single_and_multi_document/gridfs_large.bin")
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new MultiFileImportBenchmark()
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new MultiFileExportBenchmark()
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new GridFSMultiFileUploadBenchmark()
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new GridFSMultiFileDownloadBenchmark()
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
    }
}
