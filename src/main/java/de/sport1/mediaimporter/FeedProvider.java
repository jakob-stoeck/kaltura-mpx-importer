package de.sport1.mediaimporter;

import com.kaltura.client.KalturaApiException;
import com.kaltura.client.types.*;
import com.theplatform.data.api.objects.Feed;
import com.theplatform.media.api.data.objects.Media;
import com.theplatform.media.api.data.objects.MediaFile;

import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;

class FeedProvider {

    private ClientsFactory clients;

    FeedProvider(ClientsFactory clients) {
        this.clients = clients;
    }

    Feed<Media> get(KalturaMediaEntryFilter filter, KalturaFilterPager pager) throws ParseException, KalturaApiException {
        // get kaltura media
        List<KalturaMediaEntry> kalturaMediaEntries = clients.getKalturaClient().getMediaService().list(filter, pager).objects;

        // TRANSFORM ---
        // convert kaltura to mpx objects
        List<Media> mpxMedia = kalturaMediaEntries.stream().parallel().map(k -> {
            Media media = Converter.convert(k);
            // @todo restrictionId
            try {
                // add MediaFile content to Media by getting Kaltura flavors and converting them to mpx MediaFiles
                List<KalturaFlavorAssetWithParams> flavorAssets = clients.getKalturaClient().getFlavorAssetService().getFlavorAssetsWithParams(k.id);
                List<MediaFile> mediaFiles = flavorAssets.stream().parallel()
                        .map(f -> makeMediaFileWithAsset(f, k))
                        .filter(mf -> mf.getBitrate() != null)
                        .collect(Collectors.toList());
                MediaFile[] thumbnail = {Converter.thumbnail(k)};
                media.setThumbnails(thumbnail);
                MediaFile[] content = mediaFiles.toArray(new MediaFile[0]);
                media.setContent(content);
                if (0 == content.length) {
                    System.err.println(String.format(
                            "No mediafiles for %s",
                            k.id
                    ));
                }
            } catch (KalturaApiException e) {
                e.printStackTrace();
            }
            return media;
        }).collect(Collectors.toList());

        Feed<Media> feed = new Feed<>();
        feed.setEntries(mpxMedia);
        return feed;
    }

    private MediaFile makeMediaFileWithAsset(final KalturaFlavorAssetWithParams f, final KalturaMediaEntry k) {
        KalturaFlavorAsset flavorAsset = f.flavorAsset;
        KalturaFlavorParams flavorParams = f.flavorParams;
        if (flavorAsset != null) {
            try {
                String url = clients.getKalturaClient().getFlavorAssetService().getDownloadUrl(flavorAsset.id, true);
                if (url.startsWith(Converter.downloadUrlPrefix)) { // only add managed files which are on the net storage
                    return Converter.convert(k, flavorAsset, flavorParams, url);
                }
            } catch (KalturaApiException e) {
                // it's not unnatural to not have all flavours, eg for the fm podcasts
                // log error if no flavour is found at all
            }
        }
        return new MediaFile();
    }
}
