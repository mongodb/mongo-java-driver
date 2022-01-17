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
 *
 */

package com.mongodb.reactivestreams.client;

import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class JsonPoweredTestHelper {

    public static BsonDocument getTestDocument(final File file) throws IOException {
        return new BsonDocumentCodec().decode(new JsonReader(getFileAsString(file)), DecoderContext.builder().build());
    }

    public static List<File> getTestFiles(final String resourcePath) throws URISyntaxException {
        List<File> files = new ArrayList<File>();
        addFilesFromDirectory(new File(JsonPoweredTestHelper.class.getResource(resourcePath).toURI()), files);
        return files;
    }

    private static String getFileAsString(final File file) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        String ls = System.getProperty("line.separator");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
        try {
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }
        } finally {
            reader.close();
        }
        return stringBuilder.toString();
    }

    private static void addFilesFromDirectory(final File directory, final List<File> files) {
        String[] fileNames = directory.list();
        if (fileNames != null) {
            for (String fileName : fileNames) {
                File file = new File(directory, fileName);
                if (file.isDirectory()) {
                    addFilesFromDirectory(file, files);
                } else if (file.getName().endsWith(".json")) {
                    files.add(file);
                }
            }
        }
    }

    private JsonPoweredTestHelper() {
    }
}
