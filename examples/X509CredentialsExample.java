import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import javax.net.ssl.SSLSocketFactory;
import java.net.UnknownHostException;
import java.util.Arrays;

public class X509CredentialsExample {
    public static void main(String[] args) throws UnknownHostException {
        String server = args[0];
        String user = "CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US";

        System.out.println("server: " + server);
        System.out.println("user: " + user);

        System.out.println();

        MongoClient mongoClient = new MongoClient(new ServerAddress(server),
                                                  Arrays.asList(MongoCredential.createMongoX509Credential(user)),
                                                  new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault()).build());
        DB testDB = mongoClient.getDB("test");

        System.out.println("Count: " + testDB.getCollection("test").count());

        System.out.println("Insert result: " + testDB.getCollection("test").insert(new BasicDBObject()));

    }
}
