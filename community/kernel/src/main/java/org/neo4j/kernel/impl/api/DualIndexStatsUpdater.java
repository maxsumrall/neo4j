package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.store.counts.CountsStorageService;
import org.neo4j.kernel.impl.store.counts.CountsTracker;

public class DualIndexStatsUpdater implements CountsAccessor.IndexStatsUpdater
{
    private CountsAccessor.IndexStatsUpdater countsTracker;
    private CountsAccessor.IndexStatsUpdater countsStore;

    public DualIndexStatsUpdater( CountsTracker countsTracker, CountsStorageService countsStorageService )
    {
        this.countsTracker = countsTracker.updateIndexCounts();
        this.countsStore = countsStorageService.indexStatsUpdater();
    }

    @Override
    public void replaceIndexUpdateAndSize( int labelId, int propertyKeyId, long updates, long size )
    {
        System.out.println("Replace us " + labelId + " " + propertyKeyId + " " + updates + " " + size);
        countsTracker.replaceIndexUpdateAndSize( labelId, propertyKeyId, updates, size );
        countsStore.replaceIndexUpdateAndSize( labelId, propertyKeyId, updates, size );
    }

    @Override
    public void replaceIndexSample( int labelId, int propertyKeyId, long unique, long size )
    {
        System.out.println("Replace is " + labelId + " " + propertyKeyId + " " + unique + " " + size);
        countsTracker.replaceIndexSample( labelId, propertyKeyId, unique, size );
        countsStore.replaceIndexSample( labelId, propertyKeyId, unique, size );
    }

    @Override
    public void incrementIndexUpdates( int labelId, int propertyKeyId, long delta )
    {
        System.out.println("Increment " + labelId + " " + propertyKeyId + " " + delta);
        countsTracker.incrementIndexUpdates( labelId, propertyKeyId, delta );
        countsStore.incrementIndexUpdates( labelId, propertyKeyId, delta );
    }

    @Override
    public void close()
    {
        countsTracker.close();
        countsStore.close();
    }
}
