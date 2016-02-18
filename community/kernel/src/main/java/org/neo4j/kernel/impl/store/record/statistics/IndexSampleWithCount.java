package org.neo4j.kernel.impl.store.record.statistics;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;

public class IndexSampleWithCount extends StatisticsEntry
{
    private final int labelId;
    private final int propertyKeyId;
    private final long long1;
    private final long long2;

    public IndexSampleWithCount( int labelId, int propertyKeyId, long long1, long long2 )
    {
        super( CountsKeyType.INDEX_SAMPLE);
        this.labelId = labelId;
        this.propertyKeyId = propertyKeyId;
        this.long1 = long1;
        this.long2 = long2;
    }

    @Override
    public Pair<CountsKey,long[]> asSnapshotEntry()
    {
        return null;
    }

    public int getLabelId()
    {
        return labelId;
    }

    public int getPropertyKeyId()
    {
        return propertyKeyId;
    }

    public long getLong1()
    {
        return long1;
    }

    public long getLong2()
    {
        return long2;
    }
}
