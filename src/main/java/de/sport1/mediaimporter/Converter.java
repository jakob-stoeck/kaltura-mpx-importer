package de.sport1.mediaimporter;

import com.kaltura.client.enums.KalturaEntryModerationStatus;
import com.kaltura.client.types.KalturaCategory;
import com.kaltura.client.types.KalturaFlavorAsset;
import com.kaltura.client.types.KalturaFlavorParams;
import com.kaltura.client.types.KalturaMediaEntry;
import com.theplatform.data.api.objects.type.CategoryInfo;
import com.theplatform.data.api.objects.type.Duration;
import com.theplatform.media.api.data.objects.Category;
import com.theplatform.media.api.data.objects.Media;
import com.theplatform.media.api.data.objects.MediaFile;

import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

class Converter {
    static final String downloadUrlPrefix = "https://pmds.sport1.de";
    private static final URI serverId = URI.create("http://data.media.theplatform.eu/media/data/Server/81888325034");
    private static final URI serverIdThumbs = URI.create("http://data.media.theplatform.eu/media/data/Server/81888325033");
    private static final String playlistScheme = "Playlist";
    private static final Map<Integer, URI> restrictionMap = new HashMap<>();
    static Function<KalturaCategory, Category> convertCategory = kalturaCategory -> {
        Category category = new Category();
        category.setTitle(nullToEmptyString(kalturaCategory.name));
        category.setFullTitle(kalturaFullCatNameToMpx(nullToEmptyString(kalturaCategory.fullName)));
        category.setGuid(categoryImportId(kalturaCategory.id));
        category.setDescription(nullToEmptyString(kalturaCategory.description));
        category.setScheme(playlistScheme);
        return category;
    };
    static Function<String, Category> convertTagToCategory = kalturaTag -> {
        Category category = new Category();
        category.setTitle(kalturaTag);
        category.setScheme("Tag");
        return category;
    };

    static {
        // Kaltura Access Profile Id -> MPX id
        restrictionMap.put(31, URI.create("http://data.delivery.theplatform.eu/delivery/data/Restriction/44699026")); // Default
        restrictionMap.put(48, URI.create("http://data.delivery.theplatform.eu/delivery/data/Restriction/44699026")); // Global (same as default)
        restrictionMap.put(35, URI.create("http://data.delivery.theplatform.eu/delivery/data/Restriction/42651013")); // DE
        restrictionMap.put(32, URI.create("http://data.delivery.theplatform.eu/delivery/data/Restriction/45211148")); // DE, AT
        restrictionMap.put(34, URI.create("http://data.delivery.theplatform.eu/delivery/data/Restriction/42651014")); // DE, AT, CH
        restrictionMap.put(33, URI.create("http://data.delivery.theplatform.eu/delivery/data/Restriction/44699025")); // DE, AT, CH, LI, LUX
    }

    private static String nullToEmptyString(String s) {
        return s == null ? "" : s;
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
        mediaFile.setSourceUrl(getSourceUrl(url));
        mediaFile.setFileSize((long) flavorAsset.size);
        mediaFile.setAssetTypes(new String[]{"Progressive MP4"});
        mediaFile.setIsThumbnail(false);
        return mediaFile;
    }

    private static String getSourceUrl(String url) {
        return url.substring(downloadUrlPrefix.length());
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
        media.setKeywords(nullToEmptyString(kalturaMediaEntry.tags));
        if (kalturaMediaEntry.categories != null) {
            Stream<CategoryInfo> playlist = Arrays.stream(kalturaMediaEntry.categories.split(","))
                    .map(e -> convertCategoryInfo(e, playlistScheme));
            media.setCategories(playlist.toArray(CategoryInfo[]::new));
        }
        if (kalturaMediaEntry.accessControlId > 0) {
            media.setRestrictionId(restrictionMap.get(kalturaMediaEntry.accessControlId));
        }
        return media;
    }

    private static String kalturaFullCatNameToMpx(String categoryString) {
        return categoryString.replaceAll(">", "/").trim();
    }

    private static CategoryInfo convertCategoryInfo(String categoryString, String scheme) {
        CategoryInfo cat = new CategoryInfo();
        cat.setScheme(scheme);
        cat.setName((scheme.equals("Tag") ? "tag/" : "") + kalturaFullCatNameToMpx(categoryString));
        return cat;
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
