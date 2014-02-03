import com.sun.tools.doclets.Taglet;

import java.util.Map;

public class ServerReleaseTaglet extends DocTaglet {

    public static void register(Map<String, Taglet> tagletMap) {
        Taglet t = new ServerReleaseTaglet();
        tagletMap.put(t.getName(), t);
    }

    @Override
    public String getName() {
        return "mongodb.server.release";
    }

    @Override
    protected String getBaseDocURI() {
        return "http://docs.mongodb.org/manual/release-notes/";
    }

    @Override
    protected String getHeader() {
        return "Since server release";
    }
}
