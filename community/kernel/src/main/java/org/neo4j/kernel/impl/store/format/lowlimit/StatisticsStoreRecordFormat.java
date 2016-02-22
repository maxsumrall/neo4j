package org.neo4j.kernel.impl.store.format.lowlimit;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.statistics.IndexSampleWithCount;
import org.neo4j.kernel.impl.store.record.statistics.IndexStatisticsWithCount;
import org.neo4j.kernel.impl.store.record.statistics.NodeWithLabelCount;
import org.neo4j.kernel.impl.store.record.statistics.RelationshipWithLabelsCount;
import org.neo4j.kernel.impl.store.record.statistics.StatisticsEntry;
import org.neo4j.kernel.impl.store.record.statistics.StatisticsRecord;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_SAMPLE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_STATISTICS;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.value;

public class StatisticsStoreRecordFormat extends BaseOneByteHeaderRecordFormat<StatisticsRecord>
{
    //Largest possible record.
    public static final int RECORD_SIZE = Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

    public StatisticsStoreRecordFormat()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, IN_USE_BIT );
    }

    @Override
    public StatisticsRecord newRecord()
    {
        return new StatisticsRecord( -1 );
    }

    @Override
    protected void doRead( StatisticsRecord record, PageCursor cursor, int recordSize, PagedFile storeFile,
            long headerByte, boolean inUse ) throws IOException
    {
        record.setInUse( inUse );

        CountsKeyType type = value( cursor.getByte() );
        switch ( type )
        {
        case ENTITY_NODE:
            record.setEntry( new NodeWithLabelCount(
                    cursor.getInt(),
                    cursor.getLong() ) );
            break;

        case ENTITY_RELATIONSHIP:
            record.setEntry( new RelationshipWithLabelsCount(
                    cursor.getInt(),
                    cursor.getInt(),
                    cursor.getInt(),
                    cursor.getLong() ) );
            break;

        case INDEX_SAMPLE:
            record.setEntry( new IndexSampleWithCount(
                    cursor.getInt(),
                    cursor.getInt(),
                    cursor.getLong(),
                    cursor.getLong() ) );
            break;

        case INDEX_STATISTICS:
            record.setEntry( new IndexStatisticsWithCount(
                    cursor.getInt(),
                    cursor.getInt(),
                    cursor.getLong(),
                    cursor.getLong() ) );
            break;

        case EMPTY:
            throw new IllegalArgumentException( "CountsKey of type EMPTY cannot be deserialized." );

        default:
            throw new IllegalArgumentException( "The read CountsKey has an unknown type." );
        }
    }

    @Override
    protected void doWrite( StatisticsRecord record, PageCursor cursor, int recordSize, PagedFile storeFile )
            throws IOException
    {
        byte inUseByte = (record.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue();
        cursor.putByte( inUseByte );

        StatisticsEntry entry = record.getEntry();
        switch ( entry.type() )
        {
        case ENTITY_NODE:
            NodeWithLabelCount nodeWithLabelCount = (NodeWithLabelCount) entry;
            cursor.putByte( ENTITY_NODE.code );
            cursor.putInt( nodeWithLabelCount.labelId() );
            cursor.putLong( nodeWithLabelCount.count() );
            break;

        case ENTITY_RELATIONSHIP:
            RelationshipWithLabelsCount relationshipWithLabelsCount = (RelationshipWithLabelsCount) entry;
            cursor.putByte( ENTITY_RELATIONSHIP.code );
            cursor.putInt( relationshipWithLabelsCount.startLabelId() );
            cursor.putInt( relationshipWithLabelsCount.typeId() );
            cursor.putInt( relationshipWithLabelsCount.endLabelId() );
            cursor.putLong( relationshipWithLabelsCount.getCount() );
            break;

        case INDEX_SAMPLE:
            IndexSampleWithCount indexSampleWithCount = (IndexSampleWithCount) entry;
            cursor.putByte( INDEX_SAMPLE.code );
            cursor.putInt( indexSampleWithCount.getLabelId() );
            cursor.putInt( indexSampleWithCount.getPropertyKeyId() );
            cursor.putLong( indexSampleWithCount.getLong1() );
            cursor.putLong( indexSampleWithCount.getLong2() );
            break;
        case INDEX_STATISTICS:
            IndexStatisticsWithCount indexStatisticsWithCount = (IndexStatisticsWithCount) entry;
            cursor.putByte( INDEX_STATISTICS.code );
            cursor.putInt( indexStatisticsWithCount.getLabelId() );
            cursor.putInt( indexStatisticsWithCount.getPropertyKeyId() );
            cursor.putLong( indexStatisticsWithCount.getLong1() );
            cursor.putLong( indexStatisticsWithCount.getLong2() );
            break;

        case EMPTY:
            throw new IllegalArgumentException( "CountsKey of type EMPTY cannot be serialized." );

        default:
            throw new IllegalArgumentException( "The read CountsKey has an unknown type." );
        }
    }
}
