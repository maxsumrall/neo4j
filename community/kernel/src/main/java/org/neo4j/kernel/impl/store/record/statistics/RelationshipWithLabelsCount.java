package org.neo4j.kernel.impl.store.record.statistics;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;

public class RelationshipWithLabelsCount extends StatisticsEntry
{
    private final int startLabelId;
    private final int typeId;
    private final int endLabelId;
    private final long count;

    public RelationshipWithLabelsCount( int startLabelId, int typeId, int endLabelId, long count )
    {
        super( CountsKeyType.ENTITY_RELATIONSHIP);
        this.startLabelId = startLabelId;
        this.typeId = typeId;
        this.endLabelId = endLabelId;
        this.count = count;
    }

    @Override
    public Pair<CountsKey,long[]> asSnapshotEntry()
    {
        return null;
    }

    public int startLabelId()
    {
        return startLabelId;
    }

    public int typeId()
    {
        return typeId;
    }

    public int endLabelId()
    {
        return endLabelId;
    }

    public long getCount()
    {
        return count;
    }
}
