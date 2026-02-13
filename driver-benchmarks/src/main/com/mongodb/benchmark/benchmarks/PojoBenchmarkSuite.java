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

import com.mongodb.MongoClientSettings;
import com.mongodb.benchmark.benchmarks.bulk.ClientBulkWriteBenchmark;
import com.mongodb.benchmark.benchmarks.bulk.CollectionBulkWriteBenchmark;
import com.mongodb.benchmark.benchmarks.bulk.MixedClientBulkWriteBenchmark;
import com.mongodb.benchmark.benchmarks.bulk.MixedCollectionBulkWriteBenchmark;
import com.mongodb.benchmark.benchmarks.pojo.polymorphic.BranchNode;
import com.mongodb.benchmark.benchmarks.pojo.LargeDoc;
import com.mongodb.benchmark.benchmarks.pojo.TreeNode;
import com.mongodb.benchmark.benchmarks.pojo.tweet.Tweet;
import com.mongodb.benchmark.framework.Benchmark;
import com.mongodb.benchmark.framework.BenchmarkResult;
import com.mongodb.benchmark.framework.BenchmarkResultWriter;
import com.mongodb.benchmark.framework.BenchmarkRunner;
import com.mongodb.benchmark.framework.EvergreenBenchmarkResultWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.Arrays;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@SuppressWarnings({"rawtypes", "unchecked"})
public class PojoBenchmarkSuite {

    protected static final int NUM_WARMUP_ITERATIONS = 1;
    protected static final int NUM_ITERATIONS = 100;
    protected static final int MIN_TIME_SECONDS = 60;
    protected static final int MAX_TIME_SECONDS = 300;

    protected static final CodecRegistry POJO_CODEC_REGISTRY = fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build())
    );

    protected static final MongoClientSettings MONGO_CLIENT_SETTINGS = MongoClientSettings.builder()
            .codecRegistry(POJO_CODEC_REGISTRY)
            .build();

    protected static final Class<TreeNode> TREE_NODE_CLASS = TreeNode.class;
    protected static final Codec<TreeNode> TREE_NODE_CODEC = POJO_CODEC_REGISTRY.get(TREE_NODE_CLASS);

    protected static final Class<BranchNode> BRANCH_NODE_CLASS = BranchNode.class;
    protected static final Codec<BranchNode> BRANCH_NODE_CODEC = POJO_CODEC_REGISTRY.get(BRANCH_NODE_CLASS);

    protected static final Class<Tweet> TWEET_CLASS = Tweet.class;
    protected static final IdRemover<Tweet> TWEET_ID_REMOVER = tweet -> tweet.setId(null);

    protected static final Class<LargeDoc> LARGE_DOC_CLASS = LargeDoc.class;
    protected static final IdRemover<LargeDoc> LARGE_DOC_ID_REMOVER = doc -> doc.setId(null);

    protected static final List<BenchmarkResultWriter> WRITERS = Arrays.asList(
            new EvergreenBenchmarkResultWriter());

    public static void main(String[] args) throws Exception {
        runBenchmarks();

        for (BenchmarkResultWriter writer : WRITERS) {
            writer.close();
        }
    }

    private static void runBenchmarks() throws Exception {
        runBenchmark(new BsonEncodingBenchmark<>("Deep POJO", "extended_bson/deep_bson.json", TREE_NODE_CODEC));
        runBenchmark(new BsonDecodingBenchmark<>("Deep POJO", "extended_bson/deep_bson.json", TREE_NODE_CODEC));
         //TODO
//        runBenchmark(new BsonEncodingBenchmark<>("Deep POJO Polymorphic", "extended_bson/deep_bson_polymorphic.json", BRANCH_NODE_CODEC));
//        runBenchmark(new BsonDecodingBenchmark<>("Deep POJO Polymorphic", "extended_bson/deep_bson_polymorphic.json", BRANCH_NODE_CODEC));

        runBenchmark(new FindOneBenchmark<>("single_and_multi_document/tweet.json", TWEET_CLASS)
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new FindManyBenchmark<>("single_and_multi_document/tweet.json", TWEET_CLASS)
                .applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new InsertOneBenchmark<>("POJO Small", "./single_and_multi_document/tweet.json", 10_000,
                TWEET_CLASS, TWEET_ID_REMOVER).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new InsertOneBenchmark<>("POJO Large", "./single_and_multi_document/large_doc.json", 10,
                LARGE_DOC_CLASS, LARGE_DOC_ID_REMOVER).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new InsertManyBenchmark<>("POJO Small", "./single_and_multi_document/tweet.json", 10_000,
                TWEET_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new InsertManyBenchmark<>("POJO Large", "./single_and_multi_document/large_doc.json", 10,
                LARGE_DOC_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new CollectionBulkWriteBenchmark<>("POJO Small", "./single_and_multi_document/tweet.json", 10_000,
                TWEET_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new CollectionBulkWriteBenchmark<>("POJO Large", "./single_and_multi_document/large_doc.json", 10,
                LARGE_DOC_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new ClientBulkWriteBenchmark<>("POJO Small", "./single_and_multi_document/tweet.json", 10_000,
                TWEET_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new ClientBulkWriteBenchmark<>("POJO Large", "./single_and_multi_document/large_doc.json", 10,
                LARGE_DOC_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));

        runBenchmark(new MixedCollectionBulkWriteBenchmark<>("./single_and_multi_document/tweet.json", 10_000,
                TWEET_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
        runBenchmark(new MixedClientBulkWriteBenchmark<>("./single_and_multi_document/tweet.json", 10_000,
                TWEET_CLASS).applyMongoClientSettings(MONGO_CLIENT_SETTINGS));
    }

    protected static void runBenchmark(final Benchmark benchmark) throws Exception {
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