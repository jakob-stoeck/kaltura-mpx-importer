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

public class Test {

    public static void main(String[] args) throws Exception {
        // logging
        BasicConfigurator.configureDefaultContext();
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = lc.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.ERROR);

        // authentication
        ClientsFactory clients = new ClientsFactory();
        URI mpxUserId = clients.getMpxUserId();
        CategoryImporter categoryImporter = new CategoryImporter(clients);

//        deleteAllMyEntries(clients.getMpxCategoryClient(), mpxUserId);
//        categoryImporter.importCategories();
        importMedia(clients, 101);
    }

    private static void importTagsAsCategories(CategoryImporter categoryImporter) throws Exception {
        categoryImporter.initWhitelistTagsKaltura();
        categoryImporter.importTagsAsCategories();
    }

    //    private static void deleteAllMedia(ClientsFactory clients) {
//    @todo
//        clients.getMpxFileManagementClient().getService().deleteMedia();
//    }
//
    private static void deleteAllMyEntries(DataServiceClient client, URI userId) {
        Query[] queries = new Query[]{new ByAddedByUserId(userId)};
        client.delete(queries);
    }

    private static void importMedia(ClientsFactory clients, int max) throws Exception {
        String user = "http://access.auth.theplatform.com/data/Account/2691223865";
        String pass = clients.getMpxMediaClient().getAuthorization().getToken();
        PersistenceStrategy persistenceStrategy = new HttpStrategy(user, pass);
        FeedProvider feedProvider = new FeedProvider(clients);

        // which media to importMedia
        KalturaMediaEntryFilter filter = new KalturaMediaEntryFilter();
        filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_DESC.getHashCode();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        filter.createdAtGreaterThanOrEqual = Math.round(dateFormat.parse("2017-01-17T00:00:00+00:00").getTime() / 1000);
        filter.createdAtLessThanOrEqual = Math.round(dateFormat.parse("2017-01-17T23:59:59+00:00").getTime() / 1000);
        KalturaFilterPager pager = new KalturaFilterPager();
        pager.pageSize = max;
        pager.pageIndex = 1;

        // migration
        Feed<Media> feed = feedProvider.get(filter, pager);
        persistenceStrategy.persist(feed);
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
