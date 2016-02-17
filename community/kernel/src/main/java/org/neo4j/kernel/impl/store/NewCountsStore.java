package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.counts.CountsSnapshot;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.NewCountsStoreRecord;
import org.neo4j.logging.LogProvider;

public class NewCountsStore extends ComposableRecordStore<NewCountsStoreRecord,NoStoreHeader>
{
    public static final String TYPE_DESCRIPTOR = "NewCountsStore";

    public NewCountsStore( File fileName, Config configuration, IdType idType, IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache, LogProvider logProvider, RecordFormat<NewCountsStoreRecord> recordFormat,
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

    public void write( CountsSnapshot snapshot )
    {
        long highId = getHighId();
        for ( long id = 0; id < highId; id++ )
        {
            freeId( id );
        }
        try ( PagedFile file = createPagedFile( StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            // write snapshot
            file.flushAndForce();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }
}
