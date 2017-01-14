import com.kaltura.client.enums.KalturaEntryModerationStatus;
import com.kaltura.client.types.KalturaCategory;
import com.kaltura.client.types.KalturaMediaEntry;
import com.theplatform.media.api.data.objects.Category;
import com.theplatform.media.api.data.objects.Media;

import java.util.Date;
import java.util.function.Function;

class KalturaMpxConverter {
    static Function<KalturaMediaEntry, Media> convert = (kalturaMediaEntry) -> {
        Media media = new Media();
        media.setDescription(kalturaMediaEntry.description);
        if (kalturaMediaEntry.updatedAt >= 0) media.setUpdated(secondsToDate(kalturaMediaEntry.updatedAt));
        if (kalturaMediaEntry.createdAt >= 0) media.setAdded(secondsToDate(kalturaMediaEntry.createdAt));
        media.setDefaultThumbnailUrl(kalturaMediaEntry.thumbnailUrl); // I think we need to create a MediaFile first and link to its ID/URL?
        if (kalturaMediaEntry.startDate >= 0) media.setPubDate(secondsToDate(kalturaMediaEntry.startDate));
        if (kalturaMediaEntry.endDate >= 0) media.setAvailableDate(secondsToDate(kalturaMediaEntry.endDate));
        media.setTitle(kalturaMediaEntry.name);
        media.setApproved(kalturaMediaEntry.moderationStatus == KalturaEntryModerationStatus.APPROVED || kalturaMediaEntry.moderationStatus == KalturaEntryModerationStatus.AUTO_APPROVED);
        media.setGuid(kalturaMediaEntry.id);
        media.setKeywords(kalturaMediaEntry.tags);
        // @todo make HashMap<String guid, Category> to add them here
        return media;
    };
    static Function<KalturaCategory, Category> convertCategory = kalturaCategory -> {
        Category category = new Category();
        category.setTitle(kalturaCategory.fullName);
        category.setLabel(kalturaCategory.name);
        category.setGuid(importId(kalturaCategory.id));
        category.setDescription(kalturaCategory.description);
        category.setScheme("Import");
        if (kalturaCategory.createdAt >= 0) category.setAdded(secondsToDate(kalturaCategory.createdAt));
        return category;
    };

    private static Date secondsToDate(int seconds) {
        return new Date(seconds * 1000L);
    }

    static String importId(String id) {
        return "import_" + id;
    }

    static String importId(int id) {
        return importId(id + "");
    }

}
