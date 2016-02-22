package org.neo4j.kernel.impl.store.counts;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;

public class CountsStoreUpdater implements CountsAccessor.Updater
{
    private final long txId;
    private final CountsStore countsStore;

    private final Map<CountsKey,long[]> updates = new HashMap<>();

    public CountsStoreUpdater( long txId, CountsStore countsStore )
    {
        this.txId = txId;
        this.countsStore = countsStore;
    }

    @Override
    public void incrementNodeCount( int labelId, long delta )
    {
        updates.put( CountsKeyFactory.nodeKey( labelId ), new long[]{delta} );
    }

    @Override
    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        updates.put( CountsKeyFactory.relationshipKey( startLabelId, typeId, endLabelId ), new long[]{delta} );
    }

    @Override
    public void close()
    {
        countsStore.updateAll( txId, updates );
    }
}
