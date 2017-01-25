package de.sport1.mediaimporter;

import com.kaltura.client.KalturaClient;
import com.kaltura.client.KalturaConfiguration;
import com.kaltura.client.enums.KalturaSessionType;
import com.theplatform.data.api.client.ClientConfiguration;
import com.theplatform.data.api.marshalling.PayloadForm;
import com.theplatform.fms.api.client.FileManagementClient;
import com.theplatform.media.api.client.CategoryClient;
import com.theplatform.media.api.client.MediaClient;
import com.theplatform.media.api.client.ReleaseClient;
import com.theplatform.module.authentication.client.AuthenticationClient;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Properties;

class ClientsFactory {
    private final int authSessionExpiry = 1800; // seconds
    private KalturaClient kalturaClient;
    private MediaClient mpxMediaClient;
    private ReleaseClient mpxReleaseClient;
    private FileManagementClient mpxFileManagementClient;
    private CategoryClient mpxCategoryClient;
    private Properties properties;
    private String kalturaSessionId;
    private KalturaConfiguration kalturaConfiguration;
    private int kalturaPartnerId;

    ClientsFactory() throws Exception {
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
        kalturaConfiguration = new KalturaConfiguration();
        kalturaConfiguration.setEndpoint(properties.getProperty("kaltura.endpoint"));
        kalturaPartnerId = Integer.parseInt(properties.getProperty("kaltura.partnerId"));
        kalturaClient = new KalturaClient(kalturaConfiguration);
        kalturaClient.setPartnerId(kalturaPartnerId);
        kalturaSessionId = kalturaClient.generateSessionV2(
                properties.getProperty("kaltura.adminSecret"),
                properties.getProperty("kaltura.user"),
                KalturaSessionType.ADMIN,
                Integer.parseInt(properties.getProperty("kaltura.partnerId")),
                authSessionExpiry,
                ""
        );
        kalturaClient.setSessionId(kalturaSessionId);
    }

    private void authMpx() throws UnknownHostException {
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

    KalturaClient getKalturaClientUnsafe() {
        return kalturaClient;
    }

    synchronized KalturaClient getKalturaClient() {
        // KalturaClient is not thread safe. It has an internal request queue which is cleared after every request
        // You can use its multi-request functionality instead
        KalturaClient client = new KalturaClient(kalturaConfiguration);
        client.setSessionId(kalturaSessionId);
        client.setPartnerId(kalturaPartnerId);
        return client;
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
