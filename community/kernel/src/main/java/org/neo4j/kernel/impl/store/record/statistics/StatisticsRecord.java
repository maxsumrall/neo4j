package org.neo4j.kernel.impl.store.record.statistics;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class StatisticsRecord extends AbstractBaseRecord
{
    private StatisticsEntry entry;

    public StatisticsRecord( long id )
    {
        super( id );
    }

    public StatisticsRecord( long id, CountsKey key, long[] value )
    {
        super( id );
        switch ( key.recordType() )
        {
        case ENTITY_NODE:
            NodeKey nodeKey = (NodeKey) key;
            this.entry = new NodeWithLabelCount(
                    nodeKey.getLabelId(),
                    value[0] );
            break;
        case ENTITY_RELATIONSHIP:
            RelationshipKey relationshipKey = (RelationshipKey) key;
            this.entry = new RelationshipWithLabelsCount(
                    relationshipKey.getStartLabelId(),
                    relationshipKey.getTypeId(),
                    relationshipKey.getEndLabelId(),
                    value[0] );
            break;
        case INDEX_SAMPLE:
            IndexSampleKey indexSampleKey = (IndexSampleKey) key;
            this.entry = new IndexSampleWithCount(
                    indexSampleKey.labelId(),
                    indexSampleKey.propertyKeyId(),
                    value[0],
                    value[1] );
            break;
        case INDEX_STATISTICS:
            IndexStatisticsKey indexStatisticsKey = (IndexStatisticsKey) key;
            this.entry = new IndexSampleWithCount(
                    indexStatisticsKey.labelId(),
                    indexStatisticsKey.propertyKeyId(),
                    value[0],
                    value[1] );
            break;
        default:
            throw new IllegalArgumentException();
        }
    }

    public StatisticsEntry getEntry()
    {
        return entry;
    }

    public void setEntry( StatisticsEntry entry )
    {
        this.entry = entry;
    }

    @Override
    public String toString()
    {
        return "StatisticsRecord{" +
               "inUse=" + inUse() +
               ", entry=" + entry +
               '}';
    }
}
