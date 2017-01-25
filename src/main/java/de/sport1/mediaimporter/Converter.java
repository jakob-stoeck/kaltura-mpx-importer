package de.sport1.mediaimporter;

import com.kaltura.client.enums.KalturaEntryModerationStatus;
import com.kaltura.client.types.KalturaCategory;
import com.kaltura.client.types.KalturaFlavorAsset;
import com.kaltura.client.types.KalturaFlavorParams;
import com.kaltura.client.types.KalturaMediaEntry;
import com.theplatform.data.api.objects.type.Duration;
import com.theplatform.media.api.data.objects.Category;
import com.theplatform.media.api.data.objects.Media;
import com.theplatform.media.api.data.objects.MediaFile;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

class Converter {
    private static final URI serverId = URI.create("http://data.media.theplatform.eu/media/data/Server/80668741206");
    private static final URI serverIdThumbs = URI.create("http://data.media.theplatform.eu/media/data/Server/80932421069");
    static final String downloadUrlPrefix = "https://pmds.sport1.de";

    static Function<KalturaCategory, Category> convertCategory = kalturaCategory -> {
        Category category = new Category();
        category.setTitle(nullToEmptyString(kalturaCategory.fullName));
        category.setLabel(nullToEmptyString(kalturaCategory.name));
        category.setGuid(categoryImportId(kalturaCategory.id));
        category.setDescription(nullToEmptyString(kalturaCategory.description));
        category.setScheme("Import");
        return category;
    };

    private static String nullToEmptyString(String s) {
        return s == null ? "" : s;
    }

    static List<URI> convert(final KalturaMediaEntry kalturaMediaEntry, final Map<String, Category> categoryMap) {
        List<URI> categoryList = new ArrayList<>();
        if (kalturaMediaEntry.categories != null) {
            String[] kalturaCategoryIds = kalturaMediaEntry.categoriesIds.split(",");
            for (String catId : kalturaCategoryIds) {
                Category found = categoryMap.get(categoryImportId(catId));
                if (found != null) {
                    categoryList.add(found.getId());
                }
            }
        }
        return categoryList;
    }

    static MediaFile convert(final KalturaMediaEntry k, final KalturaFlavorAsset flavorAsset, final KalturaFlavorParams flavorParams, String url) {
        MediaFile mediaFile = new MediaFile();
        mediaFile.setAspectRatio((float) flavorAsset.width / flavorAsset.height);
        mediaFile.setAudioCodec(flavorParams.audioCodec.getHashCode());
        mediaFile.setVideoCodec(flavorParams.videoCodec.getHashCode());
        mediaFile.setFrameRate((float) flavorAsset.frameRate);
        mediaFile.setBitrate((long) flavorAsset.bitrate);
        mediaFile.setDuration(new Duration(k.duration));
        mediaFile.setHeight(flavorAsset.height);
        mediaFile.setWidth(flavorAsset.width);
        mediaFile.setServerId(serverId);
        mediaFile.setSourceUrl(getSourceUrl(url, true)); // remove hard-coded test parameter for live
        mediaFile.setFileSize((long) flavorAsset.size);
        mediaFile.setAssetTypes(new String[]{"Progressive MP4"});
        mediaFile.setIsThumbnail(false);
        return mediaFile;
    }

    private static String getSourceUrl(String url, boolean test) {
        return test ? url.substring((downloadUrlPrefix + "/p").length()) : url.substring(downloadUrlPrefix.length());
    }

    static MediaFile thumbnail(final KalturaMediaEntry k) {
        int width = 1920;
        int height = 1080;
        MediaFile mediaFile = new MediaFile();
        mediaFile.setAssetTypes(new String[]{"Thumbnail"});
        mediaFile.setIsThumbnail(true);
        mediaFile.setServerId(serverIdThumbs);
        mediaFile.setHeight(height);
        mediaFile.setWidth(width);
        mediaFile.setSourceUrl(String.format("%s/width/%d", k.thumbnailUrl, width));
        return mediaFile;
    }

    static Media convert(final KalturaMediaEntry kalturaMediaEntry) {
        Media media = new Media();
        media.setDescription(nullToEmptyString(kalturaMediaEntry.description));
        if (kalturaMediaEntry.createdAt >= 0) media.setPubDate(secondsToDate(kalturaMediaEntry.createdAt));
        if (kalturaMediaEntry.startDate >= 0) media.setAvailableDate(secondsToDate(kalturaMediaEntry.startDate));
        if (kalturaMediaEntry.endDate >= 0) media.setExpirationDate(secondsToDate(kalturaMediaEntry.endDate));
        media.setTitle(nullToEmptyString(kalturaMediaEntry.name));
        media.setApproved(kalturaMediaEntry.moderationStatus == KalturaEntryModerationStatus.APPROVED || kalturaMediaEntry.moderationStatus == KalturaEntryModerationStatus.AUTO_APPROVED);
        media.setGuid(kalturaMediaEntry.id);
//        media.setKeywords(kalturaMediaEntry.tags); // will be categories instead
        return media;
    }

    private static Date secondsToDate(int seconds) {
        return new Date(seconds * 1000L);
    }

    static String categoryImportId(int id) {
        return categoryImportId("" + id);
    }

    static String categoryImportId(String id) {
        return "import_" + id;
    }
}
