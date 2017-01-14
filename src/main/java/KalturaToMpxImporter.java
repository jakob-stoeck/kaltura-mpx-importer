import ch.qos.logback.classic.BasicConfigurator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.kaltura.client.KalturaClient;
import com.kaltura.client.KalturaConfiguration;
import com.kaltura.client.enums.KalturaSessionType;
import com.theplatform.data.api.Range;
import com.theplatform.data.api.client.ClientConfiguration;
import com.theplatform.data.api.marshalling.PayloadForm;
import com.theplatform.data.api.objects.DataObjectField;
import com.theplatform.data.api.objects.Feed;
import com.theplatform.data.api.objects.FieldInfo;
import com.theplatform.fms.api.client.FileManagementClient;
import com.theplatform.media.api.client.CategoryClient;
import com.theplatform.media.api.client.MediaClient;
import com.theplatform.media.api.client.ReleaseClient;
import com.theplatform.media.api.data.objects.Media;
import com.theplatform.module.authentication.client.AuthenticationClient;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Properties;

public class KalturaToMpxImporter {
    private final int authSessionExpiry = 1800; // seconds
    private KalturaClient kalturaClient;
    private MediaClient mpxMediaClient;
    private ReleaseClient mpxReleaseClient;
    private FileManagementClient mpxFileManagementClient;
    private CategoryClient mpxCategoryClient;
    private Properties properties;

    KalturaToMpxImporter() throws Exception {
        String filename = "importer.properties";
        InputStream inputStream = ClassLoader.getSystemResourceAsStream(filename);
        if (inputStream == null) {
            throw new FileNotFoundException(filename);
        }
        properties = new Properties();
        properties.load(inputStream);
        authKaltura();
        authMpx();
    }

    private void authKaltura() throws Exception {
        KalturaConfiguration config = new KalturaConfiguration();
        config.setEndpoint(properties.getProperty("kaltura.endpoint"));
        KalturaClient client = new KalturaClient(config);
        client.setPartnerId(Integer.parseInt(properties.getProperty("kaltura.partnerId")));
        client.setSessionId(client.generateSessionV2(
                properties.getProperty("kaltura.adminSecret"),
                properties.getProperty("kaltura.user"),
                KalturaSessionType.USER,
                Integer.parseInt(properties.getProperty("kaltura.partnerId")),
                authSessionExpiry,
                ""
        ));
        kalturaClient = client;
    }

    private void authMpx() throws UnknownHostException {
        BasicConfigurator.configureDefaultContext();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = lc.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.INFO);

        AuthenticationClient mpxAuthClient = new AuthenticationClient(
                properties.getProperty("mpx.baseUrl"),
                properties.getProperty("mpx.user"),
                properties.getProperty("mpx.password"),
                new String[]{properties.getProperty("mpx.accountId")}
        );
        mpxAuthClient.setTokenDuration(authSessionExpiry * 1000L);
        ClientConfiguration mpxDataConfig = new ClientConfiguration();
        mpxDataConfig.setStreamingResults(true);
        mpxDataConfig.setPayloadForm(PayloadForm.JSON);
        String dataBaseUrl = "http://data.media.theplatform.eu/media";
        mpxMediaClient = new MediaClient(dataBaseUrl, mpxAuthClient, mpxDataConfig);
        mpxReleaseClient = new ReleaseClient(dataBaseUrl, mpxAuthClient, mpxDataConfig);
        mpxCategoryClient = new CategoryClient(dataBaseUrl, mpxAuthClient, mpxDataConfig);

        com.theplatform.web.api.client.ClientConfiguration mpxWebConfig = new com.theplatform.web.api.client.ClientConfiguration();
        mpxWebConfig.setPayloadForm(com.theplatform.web.api.marshalling.PayloadForm.JSON);
        mpxFileManagementClient = new FileManagementClient("http://fms.theplatform.eu", mpxAuthClient, mpxWebConfig);
    }

    public void showSomeMpxMedia(MediaClient mpxMediaClient) {
        int rangeLowerBound = 1;
        int rangeUpperBound = 100; //limit to 100 objects
        //rangeUpperBound = Range.UNBOUNDED; // return all objects

        // request all visible Media objects
        Feed<Media> mediaList = mpxMediaClient.getAll(
                new String[]{
                        DataObjectField.id.toString(),
                        DataObjectField.title.toString(),
                        DataObjectField.customValues.toString()}, // return title, id, and custom fields
                null,   // query (default returns all visible objects)
                null,   // sort (default sorts by updated descending)
                new Range(rangeLowerBound, rangeUpperBound),   // return number of objects in range
                true);  // count total results

        System.out.println("Total results: " + mediaList.getTotalResults());

        // stream the results; print id, title, and custom values
        while (mediaList.getStreamingIterator().hasNext()) {
            Media media = mediaList.getStreamingIterator().next();
            System.out.println(media.getId() + " " + media.getTitle());

            if (media.getCustomValues() != null) {
                for (FieldInfo field : media.getCustomValues().keySet()) {
                    System.out.println("  " + field + " -> " + media.getCustomValues().get(field));
                }
            }
        }
    }

    KalturaClient getKalturaClient() {
        return kalturaClient;
    }

    MediaClient getMpxMediaClient() {
        return mpxMediaClient;
    }

    FileManagementClient getMpxFileManagementClient() {
        return mpxFileManagementClient;
    }

    CategoryClient getMpxCategoryClient() {
        return mpxCategoryClient;
    }

    ReleaseClient getMpxReleaseClient() {
        return mpxReleaseClient;
    }
}
