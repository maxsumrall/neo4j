package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;

// todo: gather updates here in a plain HashMap and flush them to the countsStore in one batch
public class CountsStoreNonTransactionalUpdater implements CountsAccessor.Updater
{
    private final CountsStore countsStore;

    public CountsStoreNonTransactionalUpdater( CountsStore countsStore )
    {
        this.countsStore = countsStore;
    }

    @Override
    public void incrementNodeCount( int labelId, long delta )
    {
        countsStore.update( CountsKeyFactory.nodeKey( labelId ), new long[]{delta} );
    }

    @Override
    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        countsStore.update( CountsKeyFactory.relationshipKey( startLabelId, typeId, endLabelId ), new long[]{delta} );
    }

    @Override
    public void close()
    {
    }
}
