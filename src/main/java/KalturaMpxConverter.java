import com.kaltura.client.enums.KalturaEntryModerationStatus;
import com.kaltura.client.types.KalturaMediaEntry;
import com.theplatform.media.api.data.objects.Media;

import java.util.Date;
import java.util.function.Function;

class KalturaMpxConverter {
    private static Date secondsToDate(int seconds) {
        return new Date(seconds * 1000L);
    }

    static Function<KalturaMediaEntry, Media> convert = kalturaMediaEntry -> {
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
        return media;
    };

}
