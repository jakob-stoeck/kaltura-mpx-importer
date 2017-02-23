package de.sport1.mediaimporter;

import com.google.common.collect.Iterables;
import com.kaltura.client.KalturaApiException;
import com.kaltura.client.enums.KalturaCategoryOrderBy;
import com.kaltura.client.types.KalturaCategory;
import com.kaltura.client.types.KalturaCategoryFilter;
import com.kaltura.client.types.KalturaFilterPager;
import com.kaltura.client.types.KalturaTagFilter;
import com.theplatform.media.api.client.CategoryClient;
import com.theplatform.media.api.data.objects.Category;
import com.theplatform.module.exception.ValidationException;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

class CategoryImporter {
    private final int maxMpxEntriesPerReq = 100;
    private final int maxKalturaPageSize = 500;
    private ClientsFactory clients;
    private Set<String> tagWhitelist = new HashSet<>();

    CategoryImporter(ClientsFactory clients) {
        this.clients = clients;
    }

    private static void removeHierarchyFromTitle(Category c) {
        // Kaltura saves the whole hierarchy in the fullTitle (e.g. A>B>C). We use the full title initially as the mpx name
        // because otherwise there could be name clashes with existing categories
        int kalturaHierarchyIndex = c.getTitle().lastIndexOf(">");
        if (kalturaHierarchyIndex != -1) {
            c.setTitle(c.getTitle().substring(kalturaHierarchyIndex + 1));
        }
    }

    void importCategories() throws KalturaApiException {
        List<KalturaCategory> kalturaCategories = getAllKalturaCategories();
        persistWithHierarchy(kalturaCategories);
    }

    private List<KalturaCategory> getAllKalturaCategories() throws KalturaApiException {
        KalturaCategoryFilter kalturaCategoryFilter = new KalturaCategoryFilter();
        kalturaCategoryFilter.orderBy = KalturaCategoryOrderBy.DEPTH_ASC.getHashCode();
        KalturaFilterPager kalturaFilterPager = new KalturaFilterPager();
        kalturaFilterPager.pageIndex = 1;
        kalturaFilterPager.pageSize = maxKalturaPageSize;
        return clients.getKalturaClient().getCategoryService().list(kalturaCategoryFilter, kalturaFilterPager).objects;
    }

    private void persistWithHierarchy(List<KalturaCategory> flatListOfCategories) {
        // saves category tree by depth level so that the parent nodes are already persisted
        Map<String, URI> fullTitleToId = new HashMap<>();
        int maxDepth = flatListOfCategories.stream().max(Comparator.comparingInt(c -> c.depth)).get().depth;
        for (int currentDepth = 0; currentDepth <= maxDepth; currentDepth++) {
            final int depth = currentDepth;
            List<Category> collect = flatListOfCategories.stream()
                    .filter(e -> e.depth == depth)
                    .map(Converter.convertCategory)
                    .map(c -> {
                        if (depth > 0) {
                            String parentFullTitle = c.getFullTitle().substring(0, c.getFullTitle().lastIndexOf('/'));
                            c.setParentId(fullTitleToId.get(parentFullTitle));
                        }
                        return c;
                    })
                    .collect(Collectors.toList());
            List<Category> current = persist(collect, new String[]{"fullTitle", "id"});
            for (Category c : current) {
                fullTitleToId.put(c.getFullTitle(), c.getId());
            }
        }
    }

    private String normalize(String s) {
        s = s.replaceAll("é", "e");
        s = s.replaceAll("è", "e");
        s = s.replaceAll("á", "a");
        s = s.replaceAll("à", "a");
        return s.trim().toLowerCase();
    }

    void initWhitelistTagsKaltura() {
        KalturaTagFilter filter = new KalturaTagFilter();
        filter.orderBy = "-instanceCount";
        KalturaFilterPager pager = new KalturaFilterPager();
        pager.pageSize = maxKalturaPageSize;
        pager.pageIndex = 1;
        while (true) {
            try {
                Set<String> kalturaTags = clients.getKalturaClient().getTagService().search(filter, pager).objects.stream()
                        .filter(e -> e.instanceCount > 40)
                        .map(e -> normalize(e.tag))
                        .collect(toSet());
                if (kalturaTags.size() == 0) {
                    break;
                }
                tagWhitelist.addAll(kalturaTags);
            } catch (KalturaApiException e) {
                System.err.println(e.getMessage());
                break;
            }
            pager.pageIndex++;
        }
    }

    void importTagsAsCategories() throws KalturaApiException {
        // put all tags under a root node named "tag"
        Category rootNode = Converter.convertTagToCategory.apply("tag");
        Category category = clients.getMpxCategoryClient().create(rootNode, new String[]{"id"});
        List<Category> transformedCategories = tagWhitelist.stream()
                .map(Converter.convertTagToCategory)
                .map(e -> {
                    e.setParentId(category.getId());
                    return e;
                })
                .collect(toList());
        persist(transformedCategories);
    }

    private void persist(List<Category> categories) {
        CategoryClient categoryClient = clients.getMpxCategoryClient();
        for (List<Category> part : Iterables.partition(categories, maxMpxEntriesPerReq)) {
            categoryClient.create(part);
        }
    }

    private List<Category> persist(List<Category> categories, String[] fields) {
        CategoryClient categoryClient = clients.getMpxCategoryClient();
        List<Category> newCategories = new ArrayList<>();
        for (List<Category> part : Iterables.partition(categories, maxMpxEntriesPerReq)) {
            try {
                newCategories.addAll(categoryClient.create(part, fields).getEntries());
            } catch (ValidationException ignore) {
                if (!ignore.getMessage().startsWith("A category with the full title ")
                        || !ignore.getMessage().endsWith(" already exists.")) {
                    throw ignore;
                }
            }
        }
        return newCategories;
    }

    boolean isWhitelisted(String name) {
        return tagWhitelist.contains(name);
    }
}
