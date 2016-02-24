/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.counts.CountsSnapshot;
import org.neo4j.kernel.impl.store.counts.CountsStore;
import org.neo4j.kernel.impl.store.counts.CountsStoreFactory;
import org.neo4j.kernel.impl.store.counts.DummyCountsStore;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.statistics.NodeWithLabelCount;
import org.neo4j.kernel.impl.store.record.statistics.StatisticsRecord;
import org.neo4j.logging.LogProvider;

// todo: write some sort of trailer to avoid reading zeroes after crash
public class StatisticsStore extends ComposableRecordStore<StatisticsRecord,NoStoreHeader>
{
    public static final String TYPE_DESCRIPTOR = "StatisticsStore";

    private CountsStore countsStore;

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
        Objects.requireNonNull( countsStore );
        CountsSnapshot snapshot = countsStore.snapshot();
        try
        {
            write( snapshot );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }

        super.flush();
    }

    public boolean readInMemoryState( CountsStoreFactory countsStoreFactory ) throws IOException
    {
        CountsSnapshot snapshot = readSnapshot();
        boolean snapshotExists = snapshot != null;
        countsStore = snapshotExists ? countsStoreFactory.create( snapshot ) : new DummyCountsStore();
        return snapshotExists;
    }

    // todo: handle all exceptions here and return null if any
    @Nullable
    public CountsSnapshot readSnapshot() throws IOException
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

    public void initializeInMemoryState( CountsStoreFactory countsStoreFactory )
    {
        countsStore = countsStoreFactory.create();
    }

    @Nonnull
    public CountsStore getCountsStore()
    {
        return Objects.requireNonNull( countsStore );
    }

    private void write( CountsSnapshot snapshot ) throws IOException
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
