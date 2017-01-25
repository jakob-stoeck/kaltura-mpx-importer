package de.sport1.mediaimporter;

import com.kaltura.client.KalturaApiException;
import com.kaltura.client.types.*;
import com.theplatform.data.api.objects.Entry;
import com.theplatform.data.api.objects.Feed;
import com.theplatform.media.api.data.objects.Category;
import com.theplatform.media.api.data.objects.Media;
import com.theplatform.media.api.data.objects.MediaFile;

import java.net.URI;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class FeedProvider {

    private ClientsFactory clients;

    FeedProvider(ClientsFactory clients) {
        this.clients = clients;
    }

    Feed<Media> get(KalturaMediaEntryFilter filter, KalturaFilterPager pager) throws ParseException, KalturaApiException {
        // get kaltura media
        List<KalturaMediaEntry> kalturaMediaEntries = clients.getKalturaClient().getMediaService().list(filter, pager).objects;

        // categories
        Map<String, Category> categoriesByGuid = clients.getMpxCategoryClient().getAll(new String[]{"guid", "id"}, null, null, null, false).getEntries().stream()
                .collect(Collectors.toMap(Entry::getGuid, Function.identity()));

        // TRANSFORM ---
        // convert kaltura to mpx objects
        List<Media> mpxMedia = kalturaMediaEntries.stream().parallel().map(k -> {
            Media media = Converter.convert(k);
            media.setCategoryIds(Converter.convert(k, categoriesByGuid).toArray(new URI[0]));
            // @todo restrictionId
            try {
                List<KalturaFlavorAssetWithParams> flavorAssets = clients.getKalturaClient().getFlavorAssetService().getFlavorAssetsWithParams(k.id);
                List<MediaFile> mediaFiles = flavorAssets.stream().parallel().map(f -> {
                    try {
                        KalturaFlavorAsset flavorAsset = f.flavorAsset;
                        KalturaFlavorParams flavorParams = f.flavorParams;
                        if (flavorAsset != null) {
                            String url = clients.getKalturaClient().getFlavorAssetService().getDownloadUrl(flavorAsset.id, true);
                            if (url.startsWith(Converter.downloadUrlPrefix)) { // only add managed files which are on the net storage
                                return Converter.convert(k, flavorAsset, flavorParams, url);
                            }
                        }
                    } catch (KalturaApiException e) {
                        e.printStackTrace();
                    }
                    return new MediaFile();
                })
                        .filter(mf -> mf.getBitrate() != null)
                        .collect(Collectors.toList());
                MediaFile[] thumbnail = {Converter.thumbnail(k)};
                media.setThumbnails(thumbnail);
                media.setContent(mediaFiles.toArray(new MediaFile[0]));
            } catch (KalturaApiException e) {
                e.printStackTrace();
            }
            return media;
        }).collect(Collectors.toList());

        Feed<Media> feed = new Feed<>();
        feed.setEntries(mpxMedia);
        return feed;
    }
}
