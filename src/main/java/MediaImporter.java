import com.kaltura.client.KalturaApiException;
import com.kaltura.client.enums.KalturaMediaEntryOrderBy;
import com.kaltura.client.types.*;
import com.theplatform.data.api.objects.DataObjectField;
import com.theplatform.fms.api.data.FileManagementService;
import com.theplatform.fms.api.data.FileResult;
import com.theplatform.fms.api.data.MediaFileInfo;
import com.theplatform.fms.api.data.Priority;
import com.theplatform.media.api.client.ReleaseClient;
import com.theplatform.media.api.data.objects.*;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


class MediaImporter {
    static void etl(final KalturaToMpxImporter importer) throws KalturaApiException {
        // EXTRACT ---
        // get kaltura objects
        // convert to mpx objects
        // send to mpx
        int pageSize = 60; // can not be over 83 (500/6) right now due to flavour filter not working with more entries
        KalturaMediaEntryFilter filter = new KalturaMediaEntryFilter();
        filter.orderBy = KalturaMediaEntryOrderBy.CREATED_AT_DESC.getHashCode();
        KalturaFilterPager pager = new KalturaFilterPager();
        pager.pageSize = pageSize;
        pager.pageIndex = 4;

        // get kaltura media
        List<KalturaMediaEntry> kalturaMediaEntries = importer.getKalturaClient().getMediaService().list(filter, pager).objects;

        // get kaltura flavor
        KalturaAssetFilter kalturaAssetFilter = new KalturaAssetFilter();
        kalturaAssetFilter.entryIdIn = kalturaMediaEntries.stream().map(k -> k.id).collect(Collectors.joining(","));
        KalturaFilterPager flavorPager = new KalturaFilterPager();
        flavorPager.pageSize = pageSize * 6; // @todo filter by flavour 0 only.
        flavorPager.pageIndex = 1;
        List<KalturaFlavorAsset> flavors = importer.getKalturaClient().getFlavorAssetService().list(kalturaAssetFilter, flavorPager).objects;

        // get urls from flavors
        Map<String, String> downloadUrls = flavors.parallelStream()
                .filter(f -> f.flavorParamsId == 0)
                .collect(Collectors.toMap(f -> f.entryId, f -> {
                    try {
                        return importer.getKalturaClient().getFlavorAssetService().getDownloadUrl(f.id);
                    } catch (KalturaApiException e) {
                        return null;
                    }
                }));

        Map<String, Category> categoriesByGuid = new HashMap<>();
        List<Category> categories = importer.getMpxCategoryClient().getAll(new String[]{"guid", "id"}, null, null, null, false).getEntries();
        for (Category c : categories) {
            categoriesByGuid.put(c.getGuid(), c);
        }

        // TRANSFORM ---
        // convert kaltura to mpx objects
        List<Media> mpxMedia = kalturaMediaEntries.stream()
                .map(k -> KalturaMpxConverter.convert(k, categoriesByGuid))
                .collect(Collectors.toList());

        // LOAD --
        // create mpx media
        String[] responseFields = new String[]{
                DataObjectField.guid.toString(),
                DataObjectField.id.toString()
        };
        List<Media> responseMedia = importer.getMpxMediaClient().create(mpxMedia, responseFields).getEntries();

        // create mpx linked media files
        FileManagementService fileManagementService = importer.getMpxFileManagementClient().getService();
        responseMedia.parallelStream()
                .forEach(m -> persistMpxMedia(m, downloadUrls, fileManagementService, importer.getMpxReleaseClient()));
    }

    private static void persistMpxMedia(final Media m, final Map<String, String> downloadUrls, final FileManagementService fileManagementService, final ReleaseClient releaseClient) {
        String entryId = m.getGuid();
        String url = downloadUrls.get(entryId);
        if (url == null) {
            System.out.printf("No flavor for %s\n", entryId);
        } else {
            try {
                URI id = m.getId();
                // linked movie
                MediaFileInfo mfi = new MediaFileInfo();
                mfi.setContentType(ContentType.video);
                mfi.setAllowRelease(true);
                TransferInfo ti = new TransferInfo();
                ti.setSupportsStreaming(true);
                ti.setSupportsDownload(true);
                mfi.setTransferInfo(ti);

                FileResult result = fileManagementService.linkNewFile(id, url, mfi, false, null, Priority.normal);
                Release release = new Release();
                release.setFileId(result.getFileId());
                release.setMediaId(id);
                release.setApproved(true);
                release.setDelivery(Delivery.download);
                releaseClient.create(release);

                // linked thumbnail
                mfi = new MediaFileInfo();
                mfi.setContentType(ContentType.image);
                mfi.setAllowRelease(true);
                ti = new TransferInfo();
                ti.setSupportsStreaming(true);
                ti.setSupportsDownload(true);
                mfi.setTransferInfo(ti);
                mfi.setIsThumbnail(true);
                mfi.setWidth(640);
                mfi.setHeight(480);
                result = fileManagementService.linkNewFile(id, String.format("http://api.medianac.com/p/120/sp/12000/thumbnail/entry_id/%s/width/640/height/480", entryId), mfi, false, null, Priority.normal);

                release = new Release();
                release.setFileId(result.getFileId());
                release.setMediaId(id);
                release.setApproved(true);
                release.setDelivery(Delivery.download);
                releaseClient.create(release);
            } catch (Exception e) {
                System.out.printf("Error with %s: %s\n", m.getGuid(), e.getMessage());
            }
        }
    }
}
