package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.counts.CountsSnapshot;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.statistics.NodeWithLabelCount;
import org.neo4j.kernel.impl.store.record.statistics.StatisticsRecord;
import org.neo4j.logging.LogProvider;

public class StatisticsStore extends ComposableRecordStore<StatisticsRecord,NoStoreHeader>
{
    public static final String TYPE_DESCRIPTOR = "StatisticsStore";

    // todo: special key is required for this
    private static final CountsKey LAST_APPLIED_TX_KEY = CountsKeyFactory.nodeKey( -42 );

    public StatisticsStore( File fileName, Config configuration, IdType idType, IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache, LogProvider logProvider, RecordFormat<StatisticsRecord> recordFormat,
            String storeVersion )
    {
        super( fileName, configuration, idType, idGeneratorFactory, pageCache, logProvider, TYPE_DESCRIPTOR,
                recordFormat, storeVersion, NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT );
    }

    @Override
    public void flush()
    {
    }

    public CountsSnapshot read() throws IOException
    {
        if ( !isInUse( 0 ) )
        {
            return null;
        }

        Map<CountsKey,long[]> map = new HashMap<>();

        StatisticsRecord reusedRecord = new StatisticsRecord( -1 );

        StatisticsRecord record = getRecord( 0, reusedRecord, RecordLoad.NORMAL );
        long txId = ((NodeWithLabelCount) record.getEntry()).count();

        for ( int i = 1; i < getHighId(); i++ )
        {
            record = getRecord( i, reusedRecord, RecordLoad.NORMAL );
            Pair<CountsKey,long[]> pair = record.getEntry().asSnapshotEntry();
            map.put( pair.first(), pair.other() );
        }
        return new CountsSnapshot( txId, map );
    }

    public void write( CountsSnapshot snapshot ) throws IOException
    {
        clearStoreFiles();

        StatisticsRecord lastTxIdRecord =
                newRecordForWrite( nextId(), LAST_APPLIED_TX_KEY, new long[]{snapshot.getTxId()} );
        updateRecord( lastTxIdRecord );

        for ( Map.Entry<CountsKey,long[]> entry : snapshot.getMap().entrySet() )
        {
            StatisticsRecord record = newRecordForWrite( nextId(), entry.getKey(), entry.getValue() );
            updateRecord( record );
        }
        storeFile.flushAndForce();
    }

    private void clearStoreFiles()
    {
        truncateAndReopenStoreFile();
        rebuildIdGenerator();
    }

    private void truncateAndReopenStoreFile()
    {
        try
        {
            storeFile.close();
            storeFile = createPagedFile( StandardOpenOption.TRUNCATE_EXISTING );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private static StatisticsRecord newRecordForWrite( long id, CountsKey key, long[] value )
    {
        StatisticsRecord record = new StatisticsRecord( id, key, value );
        record.setInUse( true );
        record.setCreated();
        record.setRequiresSecondaryUnit( false );
        return record;
    }
}
