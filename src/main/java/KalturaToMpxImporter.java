import ch.qos.logback.classic.BasicConfigurator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.kaltura.client.KalturaApiException;
import com.kaltura.client.KalturaClient;
import com.kaltura.client.KalturaConfiguration;
import com.kaltura.client.enums.KalturaMediaEntryOrderBy;
import com.kaltura.client.enums.KalturaSessionType;
import com.kaltura.client.types.*;
import com.theplatform.data.api.Range;
import com.theplatform.data.api.client.ClientConfiguration;
import com.theplatform.data.api.marshalling.PayloadForm;
import com.theplatform.data.api.objects.DataObjectField;
import com.theplatform.data.api.objects.Feed;
import com.theplatform.data.api.objects.FieldInfo;
import com.theplatform.fms.api.client.FileManagementClient;
import com.theplatform.fms.api.data.*;
import com.theplatform.media.api.client.MediaClient;
import com.theplatform.media.api.client.ReleaseClient;
import com.theplatform.media.api.data.objects.ContentType;
import com.theplatform.media.api.data.objects.Media;
import com.theplatform.media.api.data.objects.TransferInfo;
import com.theplatform.module.authentication.client.AuthenticationClient;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class KalturaToMpxImporter {
    private KalturaClient kalturaClient;
    private MediaClient mpxMediaClient;
    private ReleaseClient mpxRelease;
    private FileManagementClient mpxFileManagementClient;
    private Properties properties;
    private final int authSessionExpiry = 600; // seconds

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

    void importMedia() throws KalturaApiException {
        // EXTRACT ---
        // get kaltura objects
        // convert to mpx objects
        // send to mpx
        int pageSize = 60; // can not be over 83 (500/6) right now due to flavour filter not working with more entries
        KalturaMediaEntryFilter filter = new KalturaMediaEntryFilter();
        filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_DESC.toString();
        KalturaFilterPager pager = new KalturaFilterPager();
        pager.pageSize = pageSize;
        pager.pageIndex = 1;

        // get kaltura media
        KalturaMediaListResponse resp = kalturaClient.getMediaService().list(filter, pager);

        // get kaltura flavor
        KalturaAssetFilter kalturaAssetFilter = new KalturaAssetFilter();
        kalturaAssetFilter.entryIdIn = resp.objects.stream().map(k -> k.id).collect(Collectors.joining(","));
        KalturaFilterPager flavorPager = new KalturaFilterPager();
        flavorPager.pageSize = pageSize * 6; // @todo filter by flavour 0 only.
        flavorPager.pageIndex = 1;
        KalturaFlavorAssetListResponse flavors = kalturaClient.getFlavorAssetService().list(kalturaAssetFilter, flavorPager);

        // get urls from flavors
        Map<String, String> downloadUrls = new HashMap<>();
        for (KalturaFlavorAsset f : flavors.objects) {
            if (f.flavorParamsId != 0) {
                continue;
            }
            try {
                downloadUrls.put(f.entryId, kalturaClient.getFlavorAssetService().getDownloadUrl(f.id));
            } catch (KalturaApiException e) {
                System.out.printf("Flavor error: %s\n", e.getMessage());
            }
        }

        // TRANSFORM ---
        // convert kaltura to mpx objects
        List<Media> mpxMedia = resp.objects.stream()
                .map(KalturaMpxConverter.convert)
                .collect(Collectors.toList());

        // LOAD --
        // create mpx media
        String[] responseFields = new String[]{
                DataObjectField.guid.toString(),
                DataObjectField.id.toString()
        };
        Feed<Media> responseMedia = mpxMediaClient.create(mpxMedia, responseFields);

        // create mpx linked media files
        responseMedia.getEntries().parallelStream()
                .forEach(m -> {
                    String entryId = m.getGuid();
                    String url = downloadUrls.get(entryId);
                    if (url == null) {
                        System.out.printf("No flavor for %s\n", entryId);
                    } else {
                        try {
                            URI id = m.getId();
                            FileManagementService service = mpxFileManagementClient.getService();
                            // linked movie
                            MediaFileInfo mfi = new MediaFileInfo();
                            mfi.setContentType(ContentType.video);
                            mfi.setAllowRelease(true);
                            TransferInfo ti = new TransferInfo();
                            ti.setSupportsStreaming(true);
                            ti.setSupportsDownload(true);
                            mfi.setTransferInfo(ti);

                            FileResult result = service.linkNewFile(id, url, mfi, false, null, Priority.normal);
//                    @todo is a release needed here? i get a 403
//                    Release release = new Release();
//                    release.setFileId(result.getFileId());
//                    release.setMediaId(id);
//                    release.setApproved(true);
//                    mpxRelease.create(release);

                            // linked thumbnail
                            mfi = new MediaFileInfo();
                            mfi.setContentType(ContentType.image);
                            mfi.setAllowRelease(true);
                            mfi.setIsThumbnail(true);
                            mfi.setWidth(640);
                            mfi.setHeight(480);
                            result = service.linkNewFile(id, String.format("http://api.medianac.com/p/120/sp/12000/thumbnail/entry_id/%s/width/640/height/480", entryId), mfi, false, null, Priority.normal);

//                    release.setFileId(result.getFileId());
//                    mpxRelease.create(release);
                        } catch (FileManagementException e) {
                            System.out.printf("FileManagement error with %s: %s\n", m.getGuid(), e.getMessage());
                        }
                    }
                });
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
        mpxMediaClient = new MediaClient("http://data.media.theplatform.eu/media", mpxAuthClient, mpxDataConfig);
        mpxRelease = new ReleaseClient("http://data.media.theplatform.com/media", mpxAuthClient, mpxDataConfig);


        com.theplatform.web.api.client.ClientConfiguration mpxWebConfig = new com.theplatform.web.api.client.ClientConfiguration();
        mpxWebConfig.setPayloadForm(com.theplatform.web.api.marshalling.PayloadForm.JSON);
        mpxFileManagementClient = new FileManagementClient("http://fms.theplatform.eu", mpxAuthClient, mpxWebConfig);
    }

    public void showSomeMpxMedia() {
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
