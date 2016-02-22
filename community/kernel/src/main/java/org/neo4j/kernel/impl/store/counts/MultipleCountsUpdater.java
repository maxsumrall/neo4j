package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.api.CountsAccessor;

public class MultipleCountsUpdater implements CountsAccessor.Updater
{
    private final CountsAccessor.Updater[] updaters;

    public MultipleCountsUpdater( CountsAccessor.Updater[] updaters )
    {
        this.updaters = updaters;
    }

    @Override
    public void incrementNodeCount( int labelId, long delta )
    {
        for ( CountsAccessor.Updater updater : updaters )
        {
            updater.incrementNodeCount( labelId, delta );
        }
    }

    @Override
    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        for ( CountsAccessor.Updater updater : updaters )
        {
            updater.incrementRelationshipCount( startLabelId, typeId, endLabelId, delta );
        }
    }

    @Override
    public void close()
    {
        for ( CountsAccessor.Updater updater : updaters )
        {
            updater.close();
        }
    }
}
