import ch.qos.logback.classic.BasicConfigurator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.kaltura.client.KalturaApiException;
import com.kaltura.client.KalturaClient;
import com.kaltura.client.KalturaConfiguration;
import com.kaltura.client.enums.KalturaSessionType;
import com.kaltura.client.types.KalturaFilterPager;
import com.kaltura.client.types.KalturaMediaEntryFilter;
import com.kaltura.client.types.KalturaMediaListResponse;
import com.theplatform.data.api.Range;
import com.theplatform.data.api.client.ClientConfiguration;
import com.theplatform.data.api.marshalling.PayloadForm;
import com.theplatform.data.api.objects.DataObjectField;
import com.theplatform.data.api.objects.Feed;
import com.theplatform.data.api.objects.FieldInfo;
import com.theplatform.media.api.client.MediaClient;
import com.theplatform.media.api.data.objects.Media;
import com.theplatform.module.authentication.client.AuthenticationClient;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class KalturaToMpxImporter {
    private KalturaClient kalturaClient;
    private MediaClient mpxMediaClient;
    private AuthenticationClient mpxAuthClient;
    private ClientConfiguration mpxConfig;
    private Properties properties;
    private final int authSessionExpiry = 60; // seconds

    public KalturaToMpxImporter() throws Exception {
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

    protected void authKaltura() throws Exception {
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

    public void importMedia() throws KalturaApiException {
        // in batches:
        // get kaltura objects
        // convert to mpx objects
        // send to mpx
        KalturaMediaEntryFilter filter = new KalturaMediaEntryFilter();
        KalturaFilterPager pager = new KalturaFilterPager();
        pager.pageSize = 2;
        pager.pageIndex = 1;
        KalturaMediaListResponse resp = kalturaClient.getMediaService().list(filter, pager);
        List<Media> objects = resp.objects.stream()
                .map(KalturaMpxConverter.convert)
                .collect(Collectors.toList());
    }

    protected void authMpx() {
        BasicConfigurator.configureDefaultContext();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = lc.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.INFO);

        mpxAuthClient = new AuthenticationClient(
                properties.getProperty("mpx.baseUrl"),
                properties.getProperty("mpx.user"),
                properties.getProperty("mpx.password"),
                null);
        mpxAuthClient.setTokenDuration(authSessionExpiry * 1000L);
        mpxConfig = new ClientConfiguration();
        mpxConfig.setStreamingResults(true);
        mpxConfig.setPayloadForm(PayloadForm.JSON);
    }

    public void showSomeMpxMedia() {
        try {
            mpxMediaClient = new MediaClient("http://data.media.theplatform.eu/media", mpxAuthClient, mpxConfig);
        } catch (UnknownHostException e) {
            System.out.println("Unknown host exception: " + e.getMessage());
            return;
        }

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
}
