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

package com.mongodb.benchmark.benchmarks;

import com.mongodb.benchmark.framework.Benchmark;
import com.mongodb.benchmark.framework.BenchmarkResult;
import com.mongodb.benchmark.framework.BenchmarkResultWriter;
import com.mongodb.benchmark.framework.BenchmarkRunner;
import com.mongodb.benchmark.framework.EvergreenBenchmarkResultWriter;
import org.bson.Document;
import org.bson.codecs.Codec;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BenchmarkSuite {

    private static final int NUM_WARMUP_ITERATIONS = 1;
    private static final int NUM_ITERATIONS = 100;
    private static final int MIN_TIME_SECONDS = 60;
    private static final int MAX_TIME_SECONDS = 300;

    private static final Class DOCUMENT_CLASS = Document.class;
    private static final IdRemover<Document> ID_REMOVER = new IdRemover<Document>() {
        public void removeId(final Document document) {
            document.remove("_id");
        }
    };
    private static final Codec<Document> DOCUMENT_CODEC = getDefaultCodecRegistry().get(DOCUMENT_CLASS);

    private static final List<BenchmarkResultWriter> WRITERS = Arrays.asList(
            new EvergreenBenchmarkResultWriter());

    public static void main(String[] args) throws Exception {
        runBenchmarks();

        for (BenchmarkResultWriter writer : WRITERS) {
            writer.close();
        }
    }

    private static void runBenchmarks()
            throws Exception {

        runBenchmark(new BsonEncodingBenchmark<Document>("Flat", "extended_bson/flat_bson.json", DOCUMENT_CODEC));
        runBenchmark(new BsonEncodingBenchmark<Document>("Deep", "extended_bson/deep_bson.json", DOCUMENT_CODEC));
        runBenchmark(new BsonEncodingBenchmark<Document>("Full", "extended_bson/full_bson.json", DOCUMENT_CODEC));

        runBenchmark(new BsonDecodingBenchmark<Document>("Flat", "extended_bson/flat_bson.json", DOCUMENT_CODEC));
        runBenchmark(new BsonDecodingBenchmark<Document>("Deep", "extended_bson/deep_bson.json", DOCUMENT_CODEC));
        runBenchmark(new BsonDecodingBenchmark<Document>("Full", "extended_bson/full_bson.json", DOCUMENT_CODEC));

        runBenchmark(new RunCommandBenchmark<Document>(DOCUMENT_CODEC));
        runBenchmark(new FindOneBenchmark<Document>("single_and_multi_document/tweet.json", BenchmarkSuite.DOCUMENT_CLASS));

        runBenchmark(new InsertOneBenchmark<Document>("Small", "./single_and_multi_document/small_doc.json", 10000,
                DOCUMENT_CLASS, ID_REMOVER));
        runBenchmark(new InsertOneBenchmark<Document>("Large", "./single_and_multi_document/large_doc.json", 10,
                DOCUMENT_CLASS, ID_REMOVER));

        runBenchmark(new FindManyBenchmark<Document>("single_and_multi_document/tweet.json", BenchmarkSuite.DOCUMENT_CLASS));
        runBenchmark(new InsertManyBenchmark<Document>("Small", "./single_and_multi_document/small_doc.json", 10000,
                DOCUMENT_CLASS));
        runBenchmark(new InsertManyBenchmark<Document>("Large", "./single_and_multi_document/large_doc.json", 10,
                DOCUMENT_CLASS));

        runBenchmark(new GridFSUploadBenchmark("single_and_multi_document/gridfs_large.bin"));
        runBenchmark(new GridFSDownloadBenchmark("single_and_multi_document/gridfs_large.bin"));

        runBenchmark(new MultiFileImportBenchmark());
        runBenchmark(new MultiFileExportBenchmark());
        runBenchmark(new GridFSMultiFileUploadBenchmark());
        runBenchmark(new GridFSMultiFileDownloadBenchmark());
    }

    private static void runBenchmark(final Benchmark benchmark) throws Exception {
        long startTime = System.currentTimeMillis();
        BenchmarkResult benchmarkResult = new BenchmarkRunner(benchmark, NUM_WARMUP_ITERATIONS, NUM_ITERATIONS, MIN_TIME_SECONDS,
                MAX_TIME_SECONDS).run();
        long endTime = System.currentTimeMillis();
        System.out.println(benchmarkResult.getName() + ": " + (endTime - startTime) / 1000.0);
        for (BenchmarkResultWriter writer : WRITERS) {
            writer.write(benchmarkResult);
        }
    }
}
