package de.sport1.mediaimporter;

import com.theplatform.data.api.marshalling.Marshaller;
import com.theplatform.data.api.marshalling.MarshallingContext;
import com.theplatform.data.api.marshalling.SchemaVersion;
import com.theplatform.data.api.objects.Entry;
import com.theplatform.data.api.objects.Feed;
import sun.net.ftp.FtpClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.MessageDigest;

/**
 * Persists a Feed via FTP put
 */
public class FtpStrategy implements PersistenceStrategy {
    private final String host;
    private final int port;
    private final String user;
    private final String password;

    FtpStrategy(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public <T extends Entry> void persist(Feed<T> list, String date) throws Exception {
        Marshaller mrss = new MRSSMarshaller();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        mrss.marshal(list, stream, new MarshallingContext(new SchemaVersion(1, 8, 0), "1", true, true, true));
        byte[] bytes = stream.toByteArray();
        // in case of two or more mrss files on ftp for one day: add md5 hash
        String filename = String.format(
                "%s-%s.mrss",
                date,
                new BigInteger(1, MessageDigest.getInstance("MD5").digest(bytes)).toString(16)
        );

        FtpClient
                .create()
                .connect(new InetSocketAddress(InetAddress.getByName(host), port))
                .login(user, password.toCharArray())
                .setBinaryType()
                .putFile(filename, new ByteArrayInputStream(bytes))
                .close();
    }
}
