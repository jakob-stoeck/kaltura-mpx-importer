package de.sport1.mediaimporter;

import com.google.common.collect.Iterables;
import com.kaltura.client.KalturaApiException;
import com.kaltura.client.enums.KalturaCategoryOrderBy;
import com.kaltura.client.types.KalturaCategory;
import com.kaltura.client.types.KalturaCategoryFilter;
import com.kaltura.client.types.KalturaFilterPager;
import com.theplatform.media.api.client.CategoryClient;
import com.theplatform.media.api.data.objects.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CategoryImporter {
    public static void etl(final ClientsFactory importer) throws KalturaApiException {
        int maxMpxEntriesPerReq = 100;
        KalturaCategoryFilter kalturaCategoryFilter = new KalturaCategoryFilter();
        kalturaCategoryFilter.orderBy = KalturaCategoryOrderBy.DEPTH_ASC.getHashCode();
        KalturaFilterPager kalturaFilterPager = new KalturaFilterPager();
        kalturaFilterPager.pageIndex = 1;
        kalturaFilterPager.pageSize = 500;
        CategoryClient categoryClient = importer.getMpxCategoryClient();
        List<KalturaCategory> oldCategories = importer.getKalturaClient().getCategoryService().list(kalturaCategoryFilter, kalturaFilterPager).objects;

        // Get all kaltura categories
        // Transform and load into mpx without hierarchy because we do not have the new parent ids before saving them
        List<Category> transformedCategories = oldCategories.stream().map(Converter.convertCategory).collect(Collectors.toList());
        List<Category> newCategories = new ArrayList<>();

        for (List<Category> part : Iterables.partition(transformedCategories, maxMpxEntriesPerReq)) {
            newCategories.addAll(categoryClient.create(part, new String[]{"guid", "id"}).getEntries());
        }
        // then reload the mpx categories, which have now ids. Compare them with the kaltura categories (the old kaltura
        // id is in the guid field) and set the according parent. Re-save. This is optimized for network calls.
        List<Category> newCategoriesWithParent = newCategories.stream().map(c -> {
            KalturaCategory parentInOldCategories = oldCategories.stream()
                    .filter(oldCat -> Converter.categoryImportId(oldCat.id).equals(c.getGuid()))
                    .findAny()
                    .get();
            if (parentInOldCategories.parentId > 0) {
                // if no root node
                Category parentInNewCategories = newCategories.stream()
                        .filter(newCat -> newCat.getGuid().equals(Converter.categoryImportId(parentInOldCategories.parentId)))
                        .findAny()
                        .get();
                c.setParentId(parentInNewCategories.getId());
            }
            return c;
        }).collect(Collectors.toList());
        for (List<Category> part : Iterables.partition(newCategoriesWithParent, maxMpxEntriesPerReq)) {
            categoryClient.create(part);
        }
    }
}
