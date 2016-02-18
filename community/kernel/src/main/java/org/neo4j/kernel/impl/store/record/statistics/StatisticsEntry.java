package org.neo4j.kernel.impl.store.record.statistics;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;

public abstract class StatisticsEntry
{
    private final CountsKeyType type;

    protected StatisticsEntry( CountsKeyType type )
    {
        this.type = type;
    }

    public CountsKeyType type()
    {
        return type;
    }

    public abstract Pair<CountsKey,long[]> asSnapshotEntry();
}
