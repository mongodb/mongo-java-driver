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

package documentation;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

// Start AWS Lambda Example 1
public class ExampleAwsLambdaHandler implements RequestHandler<String, String> {
    private final MongoClient client;

    public ExampleAwsLambdaHandler() {
        client = MongoClients.create(System.getenv("MONGODB_URI"));
    }

    @Override
    public String handleRequest(final String input, final Context context) {
        return client.getDatabase("admin").runCommand(new Document("ping", 1)).toJson();
    }
}
// End AWS Lambda Example 1
