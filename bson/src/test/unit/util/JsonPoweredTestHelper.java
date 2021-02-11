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
 */

package util;

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
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JsonPoweredTestHelper {
    private static final String JSON_FILE_NAME_SUFFIX = ".json";

    public static BsonDocument getTestDocument(final File file) throws IOException {
        return new BsonDocumentCodec().decode(new JsonReader(getFileAsString(file)), DecoderContext.builder().build());
    }

    public static BsonDocument getTestDocument(final Path file) throws IOException {
        return getTestDocument(file.toFile());
    }

    public static BsonDocument getTestDocument(final String resourcePath) throws IOException, URISyntaxException {
        return getTestDocument(new File(JsonPoweredTestHelper.class.getResource(resourcePath).toURI()));
    }

    /**
     * @see #getTestFiles(String, Path...)
     */
    public static Collection<File> getTestFiles(final String resourcePath) throws URISyntaxException, IOException {
        return getTestFiles(resourcePath, (Path[]) null)
                .stream()
                .map(Path::toFile)
                .collect(Collectors.toList());
    }

    /**
     * @param resourcePath             The {@linkplain Class#getResource(String) name of a resource} representing
     *                                 a directory with test files.
     * @param excludedRelativeSubpaths Sub-paths relative to the {@code resourcePath} directory to exclude from the results.
     * @return A collection with unspecified mutability and element order.
     */
    public static Collection<Path> getTestFiles(final String resourcePath, final Path... excludedRelativeSubpaths)
            throws URISyntaxException, IOException {
        URL resource = JsonPoweredTestHelper.class.getResource(resourcePath);
        if (resource == null) {
            return Collections.emptyList();
        } else {
            Path start = Paths.get(resource.toURI());
            List<Path> excluded = excludedRelativeSubpaths == null ? Collections.emptyList() : Stream.of(excludedRelativeSubpaths)
                    .map(start::resolve)
                    .collect(Collectors.toList());
            try (Stream<Path> files = Files.walk(start)) {
                return files
                        .filter(path -> !Files.isDirectory(path))
                        .filter(path -> path.getNameCount() > 0 && path.getFileName().toString().endsWith(JSON_FILE_NAME_SUFFIX))
                        .filter(path -> excluded.stream().noneMatch(path::startsWith))
                        .collect(Collectors.toList());
            }
        }
    }

    private static String getFileAsString(final File file) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        String ls = System.getProperty("line.separator");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")));
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

    private JsonPoweredTestHelper() {
    }
}
