import com.kaltura.client.enums.KalturaEntryModerationStatus;
import com.kaltura.client.types.KalturaMediaEntry;
import com.theplatform.media.api.data.objects.Media;

import java.util.Date;
import java.util.function.Function;

public class KalturaMpxConverter {
    private static Date secondsToDate(int seconds) {
        return new Date(seconds * 1000L);
    }

    public static Function<KalturaMediaEntry, Media> convert = kalturaMediaEntry -> {
        Media media = new Media();
        media.setDescription(kalturaMediaEntry.description);
        media.setUpdated(secondsToDate(kalturaMediaEntry.updatedAt));
        media.setAdded(secondsToDate(kalturaMediaEntry.createdAt));
        media.setDefaultThumbnailUrl(kalturaMediaEntry.thumbnailUrl); // I think we need to create a MediaFile first and link to its ID/URL?
        media.setPubDate(secondsToDate(kalturaMediaEntry.startDate));
        media.setAvailableDate(secondsToDate(kalturaMediaEntry.endDate));
        media.setTitle(kalturaMediaEntry.name);
        media.setApproved(kalturaMediaEntry.moderationStatus == KalturaEntryModerationStatus.APPROVED || kalturaMediaEntry.moderationStatus == KalturaEntryModerationStatus.AUTO_APPROVED);
        return media;
    };
}
