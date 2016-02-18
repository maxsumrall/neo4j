package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.counts.CountsSnapshot;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.statistics.StatisticsRecord;
import org.neo4j.logging.LogProvider;

public class StatisticsStore extends ComposableRecordStore<StatisticsRecord,NoStoreHeader>
{
    public static final String TYPE_DESCRIPTOR = "StatisticsStore";

    public StatisticsStore( File fileName, Config configuration, IdType idType, IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache, LogProvider logProvider, RecordFormat<StatisticsRecord> recordFormat,
            String storeVersion )
    {
        super( fileName, configuration, idType, idGeneratorFactory, pageCache, logProvider, TYPE_DESCRIPTOR,
                recordFormat, storeVersion, NoStoreHeaderFormat.NO_STORE_HEADER_FORMAT );
    }

    @Override
    protected void loadStorage()
    {
    }

    @Override
    public void flush()
    {
    }

    public CountsSnapshot read() throws IOException
    {
        Map<CountsKey,long[]> map = new HashMap<>();
        StatisticsRecord reusedRecord = new StatisticsRecord( -1 );

        StatisticsRecord record = getRecord( 0, reusedRecord, RecordLoad.NORMAL );
        long txid  = record.getId();

        for ( int i = 1; i < getHighId(); i++ )
        {
            StatisticsRecord record = getRecord( i, reusedRecord, RecordLoad.NORMAL );
            Pair<CountsKey,long[]> pair = record.getEntry().asSnapshotEntry();
            map.put( pair.first(), pair.other() );
        }
        return new CountsSnapshot( 1, map );
    }

    public void write( CountsSnapshot snapshot ) throws IOException
    {
        long highId = getHighId();
        for ( long id = 0; id < highId; id++ )
        {
            freeId( id );
        }
        try ( PagedFile pagedFile = createPagedFile( StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            storeFile = pagedFile;
            long id = 0;

            StatisticsRecord lastTxIdRecord =
                    new StatisticsRecord( id, CountsKeyFactory.nodeKey( -1 ), new long[]{snapshot.getTxId()} );
            updateRecord( lastTxIdRecord );
            id++;

            for ( Map.Entry<CountsKey,long[]> entry : snapshot.getMap().entrySet() )
            {
                StatisticsRecord record = new StatisticsRecord( id, entry.getKey(), entry.getValue() );
                updateRecord( record );
                id++;
            }
            storeFile.flushAndForce();
        }
        storeFile = null;
    }
}
