package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.api.CountsAccessor;

public class NoOpUpdater implements CountsAccessor.Updater
{
    @Override
    public void incrementNodeCount( int labelId, long delta )
    {

    }

    @Override
    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {

    }

    @Override
    public void close()
    {

    }
}
