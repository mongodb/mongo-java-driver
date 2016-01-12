/*
 * Copyright 2015-2016 MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.util.JSON;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public final class JsonPoweredTestHelper {

    public static DBObject getTestDocument(final File file) throws IOException {
        return (DBObject) JSON.parse(getFileAsString(file));
    }

    public static List<File> getTestFiles(final String resourcePath) throws URISyntaxException {
        List<File> files = new ArrayList<File>();
        URL resource = JsonPoweredTestHelper.class.getResource(resourcePath);
        addFilesFromDirectory(new File(resource.toURI()), files);
        return files;
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
