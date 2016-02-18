package org.neo4j.kernel.impl.store.record.statistics;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;

public class NodeWithLabelCount extends StatisticsEntry {

    private final int labelId;
    private final long count;

    public NodeWithLabelCount( int labelId, long count )
    {
        super( CountsKeyType.ENTITY_NODE);
        this.labelId = labelId;
        this.count = count;
    }

    public int labelId()
    {
        return labelId;
    }

    public long count()
    {
        return count;
    }

    @Override
    public Pair<CountsKey,long[]> asSnapshotEntry()
    {
        return Pair.of( CountsKeyFactory.nodeKey( labelId ), new long[]{count, 1337} );
    }
}
