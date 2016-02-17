package org.neo4j.kernel.impl.store.format.lowlimit;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.NewCountsStoreRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

public class NewCountsStoreRecordFormat extends BaseRecordFormat<NewCountsStoreRecord>
{
    //Largest possible record.
    public static final int RECORD_SIZE = Byte.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES;

    public NewCountsStoreRecordFormat()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, 0 );
    }

    @Override
    public NewCountsStoreRecord newRecord()
    {
        return null;
    }

    @Override
    public void read( NewCountsStoreRecord record, PageCursor cursor, RecordLoad mode, int recordSize )
    {

    }

    @Override
    public void write( NewCountsStoreRecord record, PageCursor cursor )
    {

    }
}
