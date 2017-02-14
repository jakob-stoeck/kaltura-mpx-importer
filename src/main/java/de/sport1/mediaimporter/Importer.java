package de.sport1.mediaimporter;

import ch.qos.logback.classic.BasicConfigurator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.kaltura.client.enums.KalturaMediaEntryOrderBy;
import com.kaltura.client.types.KalturaFilterPager;
import com.kaltura.client.types.KalturaMediaEntryFilter;
import com.theplatform.data.api.client.DataServiceClient;
import com.theplatform.data.api.client.query.ByAddedByUserId;
import com.theplatform.data.api.client.query.Query;
import com.theplatform.data.api.marshalling.Marshaller;
import com.theplatform.data.api.marshalling.MarshallingContext;
import com.theplatform.data.api.marshalling.MarshallingException;
import com.theplatform.data.api.marshalling.SchemaVersion;
import com.theplatform.data.api.objects.Feed;
import com.theplatform.media.api.data.objects.Media;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Importer {

    public static void main(String[] args) throws Exception {
        // logging
        BasicConfigurator.configureDefaultContext();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = lc.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.ERROR);
        migrate();
    }

    private static void migrate() throws Exception {
        // authentication
        ClientsFactory clients = new ClientsFactory();
//        URI mpxUserId = clients.getMpxUserId();
//        CategoryImporter categoryImporter = new CategoryImporter(clients);

//        deleteAllEntriesFromUser(clients.getMpxCategoryClient(), mpxUserId);
//        categoryImporter.importCategories();
        importMedia(clients);
    }

    private static void importTagsAsCategories(CategoryImporter categoryImporter) throws Exception {
        categoryImporter.initWhitelistTagsKaltura();
        categoryImporter.importTagsAsCategories();
    }

//    @todo
//    private static void deleteAllMedia(ClientsFactory clients) {
//        clients.getMpxFileManagementClient().getService().deleteMedia();
//    }

    /**
     * Deletes all entries of a given DataServiceClient which are created by a certain user
     *
     * @param client e.g. CategoryClient
     * @param userId only elements which were created by this user are deleted
     */
    private static void deleteAllEntriesFromUser(DataServiceClient client, URI userId) {
        Query[] queries = new Query[]{new ByAddedByUserId(userId)};
        client.delete(queries);
    }

    private static void importMedia(ClientsFactory clients) throws Exception {
//        String user = clients.getMpxMediaClient().getAuthorization().getAccountIds()[0];
//        String pass = clients.getMpxMediaClient().getAuthorization().getToken();
//        PersistenceStrategy persistenceStrategy = new HttpStrategy(String.format("http://ingest.theplatform.eu/ingest/mrss?account=%s&token=%s", user, pass));

        String host = (String) clients.getPlazaFtpAuth().get("host");
        int port = Integer.parseInt((String) clients.getPlazaFtpAuth().get("port"));
        String user = (String) clients.getPlazaFtpAuth().get("user");
        String password = (String) clients.getPlazaFtpAuth().get("password");

        PersistenceStrategy persistenceStrategy = new FtpStrategy(host, port, user, password);
        FeedProvider feedProvider = new FeedProvider(clients);

        // which media to importMedia
        KalturaMediaEntryFilter filter = new KalturaMediaEntryFilter();
        filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_DESC.getHashCode();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        filter.createdAtGreaterThanOrEqual = Math.round(dateFormat.parse("2017-01-01T00:00:00+00:00").getTime() / 1000);
        filter.createdAtLessThanOrEqual = Math.round(dateFormat.parse("2017-02-15T23:59:59+00:00").getTime() / 1000);
        KalturaFilterPager pager = new KalturaFilterPager();
        pager.pageSize = 1;

        int i = 1;
        while (true) {
            // migration
            pager.pageIndex = i;
            Feed<Media> feed = feedProvider.get(filter, pager);
            if (feed.getEntries().size() == 0) {
                break;
            }
            persistenceStrategy.persist(feed);
            i++;
            break;
        }
        System.out.printf("%nImported %d videos.%n", i);
    }

    private static void testMedia() throws MarshallingException {
        Feed<Media> feed = new Feed<>();
        List<Media> items = new ArrayList<>();
        Media item = new Media();
        item.setGuid("asdf");
        item.setTitle("title");
        item.setDescription("desc");
        item.setPubDate(new Date(1485349219999L));
        items.add(item);
        feed.setEntries(items);
        Marshaller mrss = new MRSSMarshaller();
        MarshallingContext marshallingContext = new MarshallingContext(new SchemaVersion(1, 8, 0), "1", true, true, true);
        mrss.marshal(feed, System.out, marshallingContext);
    }
}
