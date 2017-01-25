package de.sport1.mediaimporter;

import com.theplatform.data.api.marshalling.Marshaller;
import com.theplatform.data.api.marshalling.MarshallingContext;
import com.theplatform.data.api.marshalling.SchemaVersion;
import com.theplatform.data.api.objects.Entry;
import com.theplatform.data.api.objects.Feed;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Persists a Feed via an HTTP Push
 * <p>
 * Other means for other strategies include:
 * - saving a file to an FTP watch folder
 * - calling the MPX FMS API
 */
public class HttpStrategy implements PersistenceStrategy {

    private final String user;
    private final String pass;

    HttpStrategy(String user, String pass) {
        this.user = user;
        this.pass = pass;
    }

    public <T extends Entry> void persist(Feed<T> list) throws Exception {
        Marshaller mrss = new MRSSMarshaller();
        MarshallingContext marshallingContext = new MarshallingContext(new SchemaVersion(1, 8, 0), "1", true, true, true);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        mrss.marshal(list, byteArrayOutputStream, marshallingContext);

        String urlString = String.format("http://ingest.theplatform.eu/ingest/mrss?account=%s&token=%s", user, pass);
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
        writer.write(byteArrayOutputStream.toString());
        writer.flush();
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        writer.close();
        reader.close();
    }
}
