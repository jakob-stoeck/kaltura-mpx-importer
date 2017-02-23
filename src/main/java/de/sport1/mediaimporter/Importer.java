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
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Importer {
    final static int KALTURA_PAGESIZE_LIMIT = 500;
    final static int PERSIST_THROTTLE_TIME = 120000;
    final static int PERSIST_THROTTLE_AFTER_ITEMS = 50;
    final static int PERSIST_DAYS_IN_ONE_GO = 1;
    final static String PERSIST_TO_DATE = "2016-01-01"; // 2009 when importing everything
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

        LocalDate date = LocalDate.now();
        LocalDate stop = LocalDate.parse(Importer.PERSIST_TO_DATE);
        long throttleThreshold = Importer.PERSIST_THROTTLE_AFTER_ITEMS;

        do {
            Date end = Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant());
            date = date.minusDays(PERSIST_DAYS_IN_ONE_GO);
            Date start = Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant());
            long importedMedia = importInterval(start, end, feedProvider, persistenceStrategy);
            throttleThreshold -= importedMedia;
            System.out.println(String.format(
                    "%d\t%s\t%s",
                    importedMedia,
                    new SimpleDateFormat("YYYY-MM-dd").format(start),
                    new SimpleDateFormat("YYYY-MM-dd").format(end)
            ));
            if (throttleThreshold < 0) {
                System.out.println("Throttling");
                throttleThreshold = Importer.PERSIST_THROTTLE_AFTER_ITEMS;
                Thread.sleep(Importer.PERSIST_THROTTLE_TIME);
            }
        } while (date.compareTo(stop) > 0);
    }

    private static long importInterval(
            Date start,
            Date end,
            FeedProvider feedProvider,
            PersistenceStrategy persistenceStrategy
    ) throws Exception {
        KalturaMediaEntryFilter filter = new KalturaMediaEntryFilter();
        filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_DESC.getHashCode();
        filter.createdAtGreaterThanOrEqual = Math.round(start.getTime() / 1000);
        filter.createdAtLessThanOrEqual = Math.round(end.getTime() / 1000 - 1);
        KalturaFilterPager pager = new KalturaFilterPager();
        pager.pageSize = Importer.KALTURA_PAGESIZE_LIMIT;

        long importedMedia = 0;
        int pageIndex = 1;

        while (true) {
            pager.pageIndex = pageIndex;
            Feed<Media> feed = feedProvider.get(filter, pager);
            if (feed.getEntries().size() == 0) {
                break;
            }
            persistenceStrategy.persist(feed, String.format(
                    "%s-%s",
                    new SimpleDateFormat("YYYY-MM-dd").format(start),
                    new SimpleDateFormat("YYYY-MM-dd").format(end)
            ));
            importedMedia += feed.getEntries().size();
            pageIndex += 1;
        }

        return importedMedia;
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
