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
import java.util.*;

public class Importer {

    final static int MAX_MRSS_IMPORT_FILES = 200;
    final static int KALTURA_PAGESIZE_LIMIT = 500;
    final static int PERSIST_THROTTLE = 30000;
    final static String FTP_PREFIX = "ftp://dsf.upload.akamai.com/";

    final static int STRATEGY_FTP = 1;
    final static int STRATEGY_HTTP = 2;
    final static int STRATEGY = Importer.STRATEGY_FTP;

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
        URI mpxUserId = clients.getMpxUserId();
        CategoryImporter categoryImporter = new CategoryImporter(clients);

        // @todo: remove
        deleteAllEntriesFromUser(clients.getMpxCategoryClient(), mpxUserId);
        // @todo: remove
        categoryImporter.importCategories();

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
        PersistenceStrategy persistenceStrategy;

        if (Importer.STRATEGY_HTTP == Importer.STRATEGY) {
            String user = clients.getMpxMediaClient().getAuthorization().getAccountIds()[0];
            String pass = clients.getMpxMediaClient().getAuthorization().getToken();
            persistenceStrategy = new HttpStrategy(String.format("http://ingest.theplatform.eu/ingest/mrss?account=%s&token=%s", user, pass));
        }

        if (Importer.STRATEGY_FTP == Importer.STRATEGY) {
            String host = (String) clients.getPlazaFtpAuth().get("host");
            int port = Integer.parseInt((String) clients.getPlazaFtpAuth().get("port"));
            String user = (String) clients.getPlazaFtpAuth().get("user");
            String password = (String) clients.getPlazaFtpAuth().get("password");
            persistenceStrategy = new FtpStrategy(host, port, user, password);
        }

        FeedProvider feedProvider = new FeedProvider(clients);

        // which media to importMedia
        KalturaMediaEntryFilter filter = new KalturaMediaEntryFilter();
        filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_DESC.getHashCode();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        filter.createdAtGreaterThanOrEqual = Math.round(dateFormat.parse("2017-01-01T00:00:00+00:00").getTime() / 1000);
        filter.createdAtLessThanOrEqual = Math.round(dateFormat.parse("2017-02-15T23:59:59+00:00").getTime() / 1000);
        KalturaFilterPager pager = new KalturaFilterPager();
        pager.pageSize = Importer.KALTURA_PAGESIZE_LIMIT;

        int pageIndex = 1;
        int entryCount = 0;
        int persistCount = 0;

        while (true) {
            pager.pageIndex = pageIndex;
            Feed<Media> feed = feedProvider.get(filter, pager);
            int count = feed.getEntries().size();

            if (count == 0) {
                break;
            }

            HashMap<String, Feed> grouped = groupByDay(feed);

            for (String date : grouped.keySet()) {
                Feed group = grouped.get(date);
                int groupCount = group.getEntries().size();
                if (groupCount > 0) {
                    if (Importer.PERSIST_THROTTLE > 0
                            && persistCount > 0
                            && persistCount % Importer.MAX_MRSS_IMPORT_FILES == 0) {
                        System.out.println("Throttling");
                        Thread.sleep(Importer.PERSIST_THROTTLE);
                    }
                    persistCount += 1;
                    entryCount += groupCount;
                    System.out.println(String.format("Going: persisting %d entries in %d puts", entryCount, persistCount));
                    persistenceStrategy.persist(group, date);
                }
            }
            pageIndex++;
//            break; // @todo: remove
        }
        System.out.println(String.format("Complete: persisted %d entries in %d puts", entryCount, persistCount));
    }

    private static HashMap<String, Feed> groupByDay(Feed<Media> feed) {
        HashMap<String, Feed> result = new HashMap<>();
        Iterator<Media> iterator = feed.getEntries().iterator();
        while (iterator.hasNext()) {
            Media media = iterator.next();
            String date = new SimpleDateFormat("YYYY-MM-dd").format(media.getPubDate());
            Feed<Media> before = result.get(date);
            Feed<Media> current = new Feed<>();
            if (before != null) {
                current.setEntries(before.getEntries());
            }
            current.getEntries().add(media);
            result.put(date, current);
        }
        return result;
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
