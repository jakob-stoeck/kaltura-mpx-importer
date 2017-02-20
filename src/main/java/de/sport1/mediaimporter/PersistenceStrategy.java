package de.sport1.mediaimporter;

import com.theplatform.data.api.objects.Entry;
import com.theplatform.data.api.objects.Feed;

public interface PersistenceStrategy {

    <T extends Entry> void persist(Feed<T> feed, String prefix) throws Exception;
}
