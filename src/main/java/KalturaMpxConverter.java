import com.kaltura.client.enums.KalturaEntryModerationStatus;
import com.kaltura.client.types.KalturaCategory;
import com.kaltura.client.types.KalturaMediaEntry;
import com.theplatform.media.api.data.objects.Category;
import com.theplatform.media.api.data.objects.Media;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class KalturaMpxConverter {
    static Function<KalturaCategory, Category> convertCategory = kalturaCategory -> {
        Category category = new Category();
        category.setTitle(kalturaCategory.fullName);
        category.setLabel(kalturaCategory.name);
        category.setGuid(importId(kalturaCategory.id));
        category.setDescription(kalturaCategory.description);
        category.setScheme("Import");
        return category;
    };

    static Media convert(final KalturaMediaEntry kalturaMediaEntry, final Map<String, Category> categoryMap) {
        Media media = convert(kalturaMediaEntry);
        if (kalturaMediaEntry.categories != null) {
            String[] kalturaCategoryIds = kalturaMediaEntry.categoriesIds.split(",");
            List<URI> categoryList = new ArrayList<>();
            for (String catId : kalturaCategoryIds) {
                Category found = categoryMap.get(importId(catId));
                if (found != null) {
                    categoryList.add(found.getId());
                }
            }
            media.setCategoryIds(categoryList.toArray(new URI[0]));
        }
        return media;
    }

    static Media convert(final KalturaMediaEntry kalturaMediaEntry) {
        Media media = new Media();
        media.setDescription(kalturaMediaEntry.description);
        if (kalturaMediaEntry.createdAt >= 0) media.setPubDate(secondsToDate(kalturaMediaEntry.createdAt));
        if (kalturaMediaEntry.startDate >= 0) media.setAvailableDate(secondsToDate(kalturaMediaEntry.startDate));
        if (kalturaMediaEntry.endDate >= 0) media.setExpirationDate(secondsToDate(kalturaMediaEntry.endDate));
        media.setTitle(kalturaMediaEntry.name);
        media.setApproved(kalturaMediaEntry.moderationStatus == KalturaEntryModerationStatus.APPROVED || kalturaMediaEntry.moderationStatus == KalturaEntryModerationStatus.AUTO_APPROVED);
        media.setGuid(kalturaMediaEntry.id);
        media.setKeywords(kalturaMediaEntry.tags);
        return media;
    }

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
