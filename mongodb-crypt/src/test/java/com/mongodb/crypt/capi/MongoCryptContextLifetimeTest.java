/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.crypt.capi;

import com.mongodb.internal.crypt.capi.MongoCrypt;
import com.mongodb.internal.crypt.capi.MongoCryptContext;
import com.mongodb.internal.crypt.capi.MongoCryptContext.State;
import com.mongodb.internal.crypt.capi.MongoCryptOptions;
import com.mongodb.internal.crypt.capi.MongoCrypts;
import com.mongodb.internal.crypt.capi.MongoKeyDecryptor;
import com.mongodb.internal.crypt.capi.MongoLocalKmsProviderOptions;
import com.mongodb.internal.crypt.capi.MongoAwsKmsProviderOptions;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * JAVA-6276: a native call must never run against a context that has been closed.
 *
 * <p>Closing a {@code MongoCryptContext} calls {@code mongocrypt_ctx_destroy}, which frees the underlying
 * {@code _mongocrypt_ctx_t} and everything libmongocrypt owns inside it — including the
 * {@code mongocrypt_kms_ctx_t} handed out by {@link MongoCryptContext#nextKeyDecryptor()}. In the reactive
 * driver the close happens from {@code doFinally} on the cancelling thread while another thread may still be
 * inside a state-machine step, so every entry point has to refuse to touch a freed handle.
 *
 * <p>These tests assert the guard rather than the memory fault: a use-after-free is not reliably observable
 * from Java, but "the call was refused" is. Against an unfixed driver the guard is absent, so the call
 * proceeds into freed native memory and the test fails either by throwing nothing or by faulting the JVM.
 */
public class MongoCryptContextLifetimeTest {

    /**
     * Every method that reaches the {@code mongocrypt_kms_ctx_t}. One case each, so a method that stops
     * refusing is reported by name rather than aborting the run before its siblings are reached.
     */
    private static Stream<Named<Consumer<MongoKeyDecryptor>>> keyDecryptorCalls() {
        return Stream.of(
                Named.of("getKmsProvider", MongoKeyDecryptor::getKmsProvider),
                Named.of("getHostName", MongoKeyDecryptor::getHostName),
                Named.of("getMessage", MongoKeyDecryptor::getMessage),
                Named.of("bytesNeeded", MongoKeyDecryptor::bytesNeeded),
                Named.of("feed", d -> d.feed(ByteBuffer.allocate(0))));
    }

    @ParameterizedTest(name = "{0} refuses after the owning context is closed")
    @MethodSource("keyDecryptorCalls")
    public void testKeyDecryptorRefusesUseAfterContextClose(final Consumer<MongoKeyDecryptor> call) {
        MongoCrypt mongoCrypt = createMongoCrypt();
        MongoCryptContext decryptor = mongoCrypt.createDecryptionContext(
                getResourceAsDocument("encrypted-command-reply.json"));

        MongoKeyDecryptor keyDecryptor = driveToKeyDecryptor(decryptor);
        assertNotNull(keyDecryptor);

        // Equivalent to Crypt's doFinally firing while KeyManagementService is still mid-round-trip: the
        // parent context is destroyed, taking the kms_ctx with it, and then the reply arrives.
        decryptor.close();

        assertThrows(IllegalStateException.class, () -> call.accept(keyDecryptor));

        mongoCrypt.close();
    }

    /**
     * The seven state-machine methods. {@code provideKmsProviderCredentials} is covered separately, in the
     * state where it is actually reachable.
     */
    private static Stream<Named<Consumer<MongoCryptContext>>> contextCalls() {
        return Stream.of(
                Named.of("getState", MongoCryptContext::getState),
                Named.of("getMongoOperation", MongoCryptContext::getMongoOperation),
                Named.of("addMongoOperationResult",
                        c -> c.addMongoOperationResult(getResourceAsDocument("key-document.json"))),
                Named.of("completeMongoOperation", MongoCryptContext::completeMongoOperation),
                Named.of("nextKeyDecryptor", MongoCryptContext::nextKeyDecryptor),
                Named.of("completeKeyDecryptors", MongoCryptContext::completeKeyDecryptors),
                Named.of("finish", MongoCryptContext::finish));
    }

    @ParameterizedTest(name = "{0} refuses after close")
    @MethodSource("contextCalls")
    public void testContextRefusesEveryCallAfterClose(final Consumer<MongoCryptContext> call) {
        MongoCrypt mongoCrypt = createMongoCrypt();
        MongoCryptContext decryptor = mongoCrypt.createDecryptionContext(
                getResourceAsDocument("encrypted-command-reply.json"));
        assertEquals(State.NEED_MONGO_KEYS, decryptor.getState());

        decryptor.close();

        assertThrows(IllegalStateException.class, () -> call.accept(decryptor));

        mongoCrypt.close();
    }

    /**
     * {@code Crypt.fetchCredentials} blocks in an HTTP round trip to a cloud
     * metadata endpoint with the context still open, cancellation frees it, and the worker thread resumes into
     * {@code mongocrypt_ctx_provide_kms_providers}.
     */
    @Test
    public void testProvideKmsProviderCredentialsRefusesUseAfterClose() {
        MongoCrypt mongoCrypt = MongoCrypts.create(MongoCryptOptions.builder()
                .kmsProviderOptions(new BsonDocument("aws", new BsonDocument()))
                .needsKmsCredentialsStateEnabled(true)
                .build());
        MongoCryptContext decryptor = mongoCrypt.createDecryptionContext(
                getResourceAsDocument("encrypted-command-reply.json"));
        assertEquals(State.NEED_KMS_CREDENTIALS, decryptor.getState());

        BsonDocument credentials = new BsonDocument("aws", new BsonDocument()
                .append("accessKeyId", new BsonString("example"))
                .append("secretAccessKey", new BsonString("example")));

        decryptor.close();

        assertThrows(IllegalStateException.class, () -> decryptor.provideKmsProviderCredentials(credentials));

        mongoCrypt.close();
    }

    @Test
    public void testCloseIsIdempotent() {
        MongoCrypt mongoCrypt = createMongoCrypt();
        MongoCryptContext decryptor = mongoCrypt.createDecryptionContext(
                getResourceAsDocument("encrypted-command-reply.json"));

        decryptor.close();
        // A second mongocrypt_ctx_destroy on the same pointer is a double free.
        decryptor.close();

        mongoCrypt.close();
    }

    private static MongoKeyDecryptor driveToKeyDecryptor(final MongoCryptContext context) {
        assertEquals(State.NEED_MONGO_KEYS, context.getState());
        context.getMongoOperation();
        context.addMongoOperationResult(getResourceAsDocument("key-document.json"));
        context.completeMongoOperation();
        assertEquals(State.NEED_KMS, context.getState());
        return context.nextKeyDecryptor();
    }

    private static BsonDocument getResourceAsDocument(final String fileName) {
        try {
            URL resource = MongoCryptContextLifetimeTest.class.getResource("/" + fileName);
            if (resource == null) {
                throw new RuntimeException("Could not find file " + fileName);
            }
            return BsonDocument.parse(new String(Files.readAllBytes(Paths.get(resource.toURI())), StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("Could not parse file " + fileName, e);
        }
    }

    private static MongoCrypt createMongoCrypt() {
        return MongoCrypts.create(MongoCryptOptions.builder()
                .awsKmsProviderOptions(MongoAwsKmsProviderOptions.builder()
                        .accessKeyId("example")
                        .secretAccessKey("example")
                        .build())
                .localKmsProviderOptions(MongoLocalKmsProviderOptions.builder()
                        .localMasterKey(ByteBuffer.wrap(new byte[96]))
                        .build())
                .build());
    }

}
