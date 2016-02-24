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
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * Contains common implementation of {@link RecordStore}.
 */
public abstract class CommonAbstractStore<RECORD extends AbstractBaseRecord>
        implements RecordStore<RECORD>, AutoCloseable
{
    public static final String UNKNOWN_VERSION = "Unknown";

    protected final Config configuration;
    protected final PageCache pageCache;
    protected final File storageFileName;
    protected final IdType idType;
    protected final IdGeneratorFactory idGeneratorFactory;
    protected final Log log;
    protected PagedFile storeFile;
    private IdGenerator idGenerator;
    private boolean storeOk = true;
    private Throwable causeOfStoreNotOk;
    private final String typeDescriptor;

    private final String storeVersion;

    /**
     * Opens and validates the store contained in <CODE>fileName</CODE>
     * loading any configuration defined in <CODE>config</CODE>. After
     * validation the <CODE>initStorage</CODE> method is called.
     * <p>
     * If the store had a clean shutdown it will be marked as <CODE>ok</CODE>
     * and the {@link #getStoreOk()} method will return true.
     * If a problem was found when opening the store the {@link #makeStoreOk()}
     * must be invoked.
     * <p>
     * throws IOException if the unable to open the storage or if the
     * <CODE>initStorage</CODE> method fails
     *
     * @param idType The Id used to index into this store
     */
    public CommonAbstractStore(
            File fileName,
            Config configuration,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            String typeDescriptor,
            String storeVersion )
    {
        this.storageFileName = fileName;
        this.configuration = configuration;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.idType = idType;
        this.typeDescriptor = typeDescriptor;
        this.storeVersion = storeVersion;
        this.log = logProvider.getLog( getClass() );
    }

    protected static long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

    void initialise( boolean createIfNotExists )
    {
        try
        {
            checkStorage( createIfNotExists );
            loadStorage();
        }
        catch ( Exception e )
        {
            if ( storeFile != null )
            {
                try
                {
                    closeStoreFile();
                }
                catch ( IOException failureToClose )
                {
                    // Not really a suppressed exception, but we still want to throw the real exception, e,
                    // but perhaps also throw this in there or convenience.
                    e.addSuppressed( failureToClose );
                }
            }
            throw launderedException( e );
        }
    }

    /**
     * Returns the type and version that identifies this store.
     *
     * @return This store's implementation type and version identifier
     */
    public String getTypeDescriptor()
    {
        return typeDescriptor;
    }

    /**
     * This method is called by constructors.
     * @param createIfNotExists If true, creates and initialises the store file if it does not exist already. If false,
     * this method will instead throw an exception in that situation.
     */
    protected void checkStorage( boolean createIfNotExists )
    {
        try ( PagedFile ignore = pageCache.map( storageFileName, pageCache.pageSize() ) )
        {
        }
        catch ( NoSuchFileException e )
        {
            if ( createIfNotExists )
            {
                try ( PagedFile file = pageCache.map( storageFileName, pageCache.pageSize(), StandardOpenOption.CREATE ) )
                {
                    initialiseNewStoreFile( file );
                    return;
                }
                catch ( IOException e1 )
                {
                    e.addSuppressed( e1 );
                }
            }
            throw new StoreNotFoundException( "Store file not found: " + storageFileName, e );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to open store file: " + storageFileName, e );
        }
    }

    protected void initialiseNewStoreFile( PagedFile file ) throws IOException
    {
        if ( getNumberOfReservedLowIds() > 0 )
        {
            try ( PageCursor pageCursor = file.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                if ( pageCursor.next() )
                {
                    do
                    {
                        pageCursor.setOffset( 0 );
                        createHeaderRecord( pageCursor );
                    }
                    while ( pageCursor.shouldRetry() );
                }
            }
        }

        File idFileName = new File( storageFileName.getPath() + ".id" );
        idGeneratorFactory.create( idFileName, getNumberOfReservedLowIds(), true );
    }

    protected void createHeaderRecord( PageCursor cursor )
    {
        assert getNumberOfReservedLowIds() == 0;
    }

    /**
     * Should do first validation on store validating stuff like version and id
     * generator. This method is called by constructors.
     * <p>
     * Note: This method will map the file with the page cache. The store file must not
     * be accessed directly until it has been unmapped - the store file must only be
     * accessed through the page cache.
     */
    protected void loadStorage()
    {
        try
        {
            extractHeaderRecord();
            storeFile = createPagedFile();
        }
        catch ( IOException e )
        {
            // TODO: Just throw IOException, add proper handling further up
            throw new UnderlyingStorageException( e );
        }
        loadIdGenerator();
    }

    protected PagedFile createPagedFile( OpenOption... openOptions ) throws IOException
    {
        int filePageSize = pageCache.pageSize() - pageCache.pageSize() % getRecordSize();
        return pageCache.map( getStorageFileName(), filePageSize, openOptions );
    }

    private void extractHeaderRecord() throws IOException
    {
        if ( getNumberOfReservedLowIds() > 0 )
        {
            try ( PagedFile pagedFile = pageCache.map( getStorageFileName(), pageCache.pageSize() ) )
            {
                try ( PageCursor pageCursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
                {
                    if ( pageCursor.next() )
                    {
                        do
                        {
                            pageCursor.setOffset( 0 );
                            readHeaderAndInitializeRecordFormat( pageCursor );
                        }
                        while ( pageCursor.shouldRetry() );
                    }
                }
            }
        }
        else
        {
            readHeaderAndInitializeRecordFormat( null );
        }
    }

    protected long pageIdForRecord( long id )
    {
        return RecordPageLocationCalculator.pageIdForRecord( id, storeFile.pageSize(), getRecordSize() );
    }

    protected int offsetForId( long id )
    {
        return RecordPageLocationCalculator.offsetForId( id, storeFile.pageSize(), getRecordSize() );
    }

    @Override
    public int getRecordsPerPage()
    {
        return storeFile.pageSize() / getRecordSize();
    }

    public byte[] getRawRecordData( long id ) throws IOException
    {
        byte[] data = new byte[getRecordSize()];
        try ( PageCursor pageCursor = storeFile.io( id / getRecordsPerPage(), PagedFile.PF_SHARED_READ_LOCK ) )
        {
            if ( pageCursor.next() )
            {
                do
                {
                    pageCursor.setOffset( (int) (id % getRecordsPerPage() * getRecordSize()) );
                    pageCursor.getBytes( data );
                }
                while ( pageCursor.shouldRetry() );
            }
        }
        return data;
    }


    /**
     * This method is called when opening the store to extract header data and determine things like
     * record size of the specific record format for this store. Some formats rely on information
     * in the store header, that's why it happens at this stage.
     *
     * @param cursor {@link PageCursor} initialized at the start of the store header where header information
     * can be read if need be. This can be {@code null} if this store has no store header. The initialization
     * of the record format still happens in here.
     * @throws IOException if there were problems reading header information.
     */
    protected void readHeaderAndInitializeRecordFormat( PageCursor cursor ) throws IOException
    {
    }

    private void loadIdGenerator()
    {
        try
        {
            if ( storeOk )
            {
                openIdGenerator();
            }
            // else we will rebuild the id generator after recovery, and we don't want to have the id generator
            // picking up calls to freeId during recovery.
        }
        catch ( InvalidIdGeneratorException e )
        {
            setStoreNotOk( e );
        }
        finally
        {
            if ( !getStoreOk() )
            {
                log.debug( getStorageFileName() + " non clean shutdown detected" );
            }
        }
    }

    protected int getHeaderRecord() throws IOException
    {
        int headerRecord = 0;
        try ( PagedFile pagedFile = pageCache.map( getStorageFileName(), pageCache.pageSize() ) )
        {
            try ( PageCursor pageCursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                if ( pageCursor.next() )
                {
                    do
                    {
                        headerRecord = pageCursor.getInt();
                    }
                    while ( pageCursor.shouldRetry() );
                }
            }
        }
        if ( headerRecord <= 0 )
        {
            throw new InvalidRecordException( "Illegal block size: " +
                    headerRecord + " in " + getStorageFileName() );
        }
        return headerRecord;
    }

    public boolean isInUse( long id )
    {
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );

        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            boolean recordIsInUse = false;
            if ( cursor.next() )
            {
                do
                {
                    cursor.setOffset( offset );
                    recordIsInUse = isInUse( cursor );
                }
                while ( cursor.shouldRetry() );
            }
            return recordIsInUse;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    /**
     * Should rebuild the id generator from scratch.
     * <p>
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementers must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    final void rebuildIdGenerator()
    {
        int blockSize = getRecordSize();
        if ( blockSize <= 0 )
        {
            throw new InvalidRecordException( "Illegal blockSize: " + blockSize );
        }

        log.info( "Rebuilding id generator for[" + getStorageFileName() + "] ..." );
        closeIdGenerator();
        createIdGenerator( getIdFileName() );
        openIdGenerator();

        long defraggedCount = 0;
        boolean fastRebuild = doFastIdGeneratorRebuild();

        try
        {
            long foundHighId = scanForHighId();
            setHighId( foundHighId );
            if ( !fastRebuild )
            {
                try ( PageCursor cursor = storeFile.io( 0, PF_SHARED_WRITE_LOCK | PF_READ_AHEAD ) )
                {
                    defraggedCount = rebuildIdGeneratorSlow( cursor, getRecordsPerPage(), blockSize, foundHighId );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to rebuild id generator " + getStorageFileName(), e );
        }

        log.info( "[" + getStorageFileName() + "] high id=" + getHighId() + " (defragged=" + defraggedCount + ")" );
        log.info( getStorageFileName() + " rebuild id generator, highId=" + getHighId() +
                  " defragged count=" + defraggedCount );

        if ( !fastRebuild )
        {
            closeIdGenerator();
            openIdGenerator();
        }
    }

    private long rebuildIdGeneratorSlow( PageCursor cursor, int recordsPerPage, int blockSize,
                                         long foundHighId )
            throws IOException
    {
        long defragCount = 0;
        long[] freedBatch = new long[recordsPerPage]; // we process in batches of one page worth of records
        int startingId = getNumberOfReservedLowIds();
        int defragged;

        boolean done = false;
        while ( !done && cursor.next() )
        {
            long idPageOffset = (cursor.getCurrentPageId() * recordsPerPage);

            do
            {
                defragged = 0;
                done = false;
                for ( int i = startingId; i < recordsPerPage; i++ )
                {
                    int offset = i * blockSize;
                    cursor.setOffset( offset );
                    long recordId = idPageOffset + i;
                    if ( recordId >= foundHighId )
                    {   // We don't have to go further than the high id we found earlier
                        done = true;
                        break;
                    }

                    if ( !isInUse( cursor ) )
                    {
                        freedBatch[defragged++] = recordId;
                    }
                    else if ( isRecordReserved( cursor ) )
                    {
                        cursor.setOffset( offset );
                        cursor.putByte( Record.NOT_IN_USE.byteValue() );
                        cursor.putInt( 0 );
                        freedBatch[defragged++] = recordId;
                    }
                }
            }
            while ( cursor.shouldRetry() );

            for ( int i = 0; i < defragged; i++ )
            {
                freeId( freedBatch[i] );
            }
            defragCount += defragged;
            startingId = 0;
        }
        return defragCount;
    }

    protected boolean doFastIdGeneratorRebuild()
    {
        return configuration.get( Configuration.rebuild_idgenerators_fast );
    }

    /**
     * Marks this store as "not ok".
     */
    protected void setStoreNotOk( Throwable cause )
    {
        storeOk = false;
        causeOfStoreNotOk = cause;
        idGenerator = null; // since we will rebuild it later
    }

    /**
     * If store is "not ok" <CODE>false</CODE> is returned.
     *
     * @return True if this store is ok
     */
    protected boolean getStoreOk()
    {
        return storeOk;
    }

    /**
     * Throws cause of not being OK if {@link #getStoreOk()} returns {@code false}.
     */
    protected void checkStoreOk()
    {
        if ( !storeOk )
        {
            throw launderedException( causeOfStoreNotOk );
        }
    }

    /**
     * Returns the next id for this store's {@link IdGenerator}.
     *
     * @return The next free id
     */
    @Override
    public long nextId()
    {
        return idGenerator.nextId();
    }

    /**
     * Frees an id for this store's {@link IdGenerator}.
     *
     * @param id The id to free
     */
    public void freeId( long id )
    {
        if ( idGenerator != null )
        {
            idGenerator.freeId( id );
        }
        // else we're deleting records as part of applying transactions during recovery, and that's fine
    }

    /**
     * Return the highest id in use. If this store is not OK yet, the high id is calculated from the highest
     * in use record on the store, using {@link #scanForHighId()}.
     *
     * @return The high id, i.e. highest id in use + 1.
     */
    @Override
    public long getHighId()
    {
        return idGenerator != null ? idGenerator.getHighId() : scanForHighId();
    }

    /**
     * Sets the high id, i.e. highest id in use + 1 (use this when rebuilding id generator).
     *
     * @param highId The high id to set.
     */
    public void setHighId( long highId )
    {
        // This method might get called during recovery, where we don't have a reliable id generator yet,
        // so ignore these calls and let rebuildIdGenerators() figure out the high id after recovery.
        if ( idGenerator != null )
        {
            synchronized ( idGenerator )
            {
                if ( highId > idGenerator.getHighId() )
                {
                    idGenerator.setHighId( highId );
                }
            }
        }
    }

    /**
     * If store is not ok a call to this method will rebuild the {@link
     * IdGenerator} used by this store and if successful mark it as OK.
     *
     * WARNING: this method must NOT be called if recovery is required, but hasn't performed.
     * To remove all negations from the above statement: Only call this method if store is in need of
     * recovery and recovery has been performed.
     */
    public void makeStoreOk()
    {
        if ( !storeOk )
        {
            rebuildIdGenerator();
            storeOk = true;
            causeOfStoreNotOk = null;
        }
    }

    /**
     * Returns the name of this store.
     *
     * @return The name of this store
     */
    @Override
    public File getStorageFileName()
    {
        return storageFileName;
    }

    private File getIdFileName()
    {
        return new File( getStorageFileName().getPath() + ".id" );
    }

    /**
     * Opens the {@link IdGenerator} used by this store.
     * <p>
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementers must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    protected void openIdGenerator()
    {
        idGenerator = openIdGenerator( getIdFileName(), idType.getGrabSize() );
    }

    /**
     * Opens the {@link IdGenerator} given by the fileName.
     * <p>
     * Note: This method may be called both while the store has the store file mapped in the
     * page cache, and while the store file is not mapped. Implementers must therefore
     * map their own temporary PagedFile for the store file, and do their file IO through that,
     * if they need to access the data in the store file.
     */
    protected IdGenerator openIdGenerator( File fileName, int grabSize )
    {
        return idGeneratorFactory.open( fileName, grabSize, getIdType(), scanForHighId() );
    }

    /**
     * Starts from the end of the file and scans backwards to find the highest in use record.
     * Can be used even if {@link #makeStoreOk()} hasn't been called. Basically this method should be used
     * over {@link #getHighestPossibleIdInUse()} and {@link #getHighId()} in cases where a store has been opened
     * but is in a scenario where recovery isn't possible, like some tooling or migration.
     *
     * @return the id of the highest in use record + 1, i.e. highId.
     */
    protected long scanForHighId()
    {
        try ( PageCursor cursor = storeFile.io( 0, PF_SHARED_READ_LOCK ) )
        {
            long nextPageId = storeFile.getLastPageId();
            int recordsPerPage = getRecordsPerPage();
            int recordSize = getRecordSize();
            while ( nextPageId >= 0 && cursor.next( nextPageId ) )
            {
                nextPageId--;
                do
                {
                    int currentRecord = recordsPerPage;
                    while ( currentRecord-- > 0 )
                    {
                        cursor.setOffset( currentRecord * recordSize );
                        long recordId = (cursor.getCurrentPageId() * recordsPerPage) + currentRecord;
                        if ( isInUse( cursor ) )
                        {
                            // We've found the highest id in use
                            return recordId + 1 /*+1 since we return the high id*/;
                        }
                    }
                }
                while ( cursor.shouldRetry() );
            }

            return getNumberOfReservedLowIds();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to find high id by scanning backwards " + getStorageFileName(), e );
        }
    }

    @Override
    public abstract int getRecordSize();

    @Override
    public int getRecordDataSize()
    {
        return getRecordSize();
    }

    protected abstract boolean isInUse( PageCursor cursor );

    protected boolean isRecordReserved( PageCursor cursor )
    {
        return false;
    }

    protected void createIdGenerator( File fileName )
    {
        idGeneratorFactory.create( fileName, 0, false );
    }

    /** Closed the {@link IdGenerator} used by this store */
    protected void closeIdGenerator()
    {
        if ( idGenerator != null )
        {
            idGenerator.close();
        }
    }

    @Override
    public void flush()
    {
        try
        {
            storeFile.flushAndForce();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to flush", e );
        }
    }

    /**
     * Checks if this store is closed and throws exception if it is.
     *
     * @throws IllegalStateException if the store is closed
     */
    protected void assertNotClosed()
    {
        if ( storeFile == null )
        {
            throw new IllegalStateException( this + " for file '" + storageFileName + "' is closed" );
        }
    }

    /**
     * Closes this store. This will cause all buffers and channels to be closed.
     * Requesting an operation from after this method has been invoked is
     * illegal and an exception will be thrown.
     * <p>
     * This method will start by invoking the {@link #closeStorage} method
     * giving the implementing store way to do anything that it needs to do
     * before the pagedFile is closed.
     */
    @Override
    public void close()
    {
        if ( idGenerator == null || !storeOk )
        {
            try
            {
                closeStoreFile();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Failed to close store file: " + getStorageFileName(), e );
            }
            return;
        }
        try
        {
            closeStoreFile();
        }
        catch ( IOException | IllegalStateException e )
        {
            throw new UnderlyingStorageException( "Failed to close store file: " + getStorageFileName(), e );
        }
    }

    private void closeStoreFile() throws IOException
    {
        try
        {
            /*
             * Note: the closing ordering here is important!
             * It is the case since we wand to mark the id generator as closed cleanly ONLY IF
             * also the store file is cleanly shutdown.
             */
            storeFile.close();
            if ( idGenerator != null )
            {
                idGenerator.close();
            }
        }
        finally
        {
            storeFile = null;
        }
    }

    /** @return The highest possible id in use, -1 if no id in use. */
    @Override
    public long getHighestPossibleIdInUse()
    {
        return idGenerator != null ? idGenerator.getHighestPossibleIdInUse() : scanForHighId() - 1;
    }

    /**
     * Sets the highest id in use. After this call highId will be this given id + 1.
     *
     * @param highId The highest id in use to set.
     */
    @Override
    public void setHighestPossibleIdInUse( long highId )
    {
        setHighId( highId + 1 );
    }

    /** @return The total number of ids in use. */
    public long getNumberOfIdsInUse()
    {
        return idGenerator.getNumberOfIdsInUse();
    }

    /**
     * @return the number of records at the beginning of the store file that are reserved for other things
     * than actual records. Stuff like permanent configuration data.
     */
    @Override
    public int getNumberOfReservedLowIds()
    {
        return 0;
    }

    public IdType getIdType()
    {
        return idType;
    }

    public void logVersions( Logger logger )
    {
        logger.log( "  " + getTypeDescriptor() + " " + storeVersion );
    }

    public void logIdUsage( Logger logger )
    {
        logger.log( String.format( "  %s: used=%s high=%s",
                getTypeDescriptor(), getNumberOfIdsInUse(), getHighestPossibleIdInUse() ) );
    }

    /**
     * Visits this store, and any other store managed by this store.
     * TODO this could, and probably should, replace all override-and-do-the-same-thing-to-all-my-managed-stores
     * methods like:
     * {@link #makeStoreOk()},
     * {@link #close()} (where that method could be deleted all together and do a visit in {@link #close()}),
     * {@link #logIdUsage(Logger)},
     * {@link #logVersions(Logger)}
     * For a good samaritan to pick up later.
     */
    public void visitStore( Visitor<CommonAbstractStore,RuntimeException> visitor )
    {
        visitor.visit( this );
    }

    /**
     * Called from the part of the code that starts the {@link MetaDataStore} and friends, together with any
     * existing transaction log, seeing that there are transactions to recover. Now, this shouldn't be
     * needed because the state of the id generator _should_ reflect this fact, but turns out that,
     * given HA and the nature of the .id files being like orphans to the rest of the store, we just
     * can't trust that to be true. If we happen to have id generators open during recovery we delegate
     * {@link #freeId(long)} calls to {@link IdGenerator#freeId(long)} and since the id generator is most likely
     * out of date w/ regards to high id, it may very well blow up.
     */
    final void deleteIdGenerator()
    {
        if ( idGenerator != null )
        {
            idGenerator.delete();
            idGenerator = null;
        }
    }

    @Override
    public long getNextRecordReference( RECORD record )
    {
        return Record.NULL_REFERENCE.intValue();
    }

    /**
     * Acquires a {@link PageCursor} from the {@link PagedFile store file} and reads the requested record
     * in the correct page and offset.
     *
     * @param id the record id.
     * @param record the record instance to load the data into.
     * @param mode how strict to be when loading, f.ex {@link RecordLoad#FORCE} will always read what's there
     * and load into the record, whereas {@link RecordLoad#NORMAL} will throw {@link InvalidRecordException}
     * if not in use.
     */
    @Override
    public RECORD getRecord( long id, RECORD record, RecordLoad mode )
    {
        long pageId = pageIdForRecord( id );
        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            return getRecord( id, record, mode, cursor, pageId );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    protected RECORD getRecord( long id, RECORD record, RecordLoad mode, PageCursor cursor, long pageId )
    {
        int offset = offsetForId( id );
        try
        {
            if ( cursor.next( pageId ) )
            {
                // There is a page in the store that covers this record, go read it
                readRecordWithRetry( cursor, id, record, mode, offset );
            }
            else
            {
                // There was no page in the store covering this record. We mark the record with
                // the correct id because often the caller depends on the id to be correct regardless
                // of whether the record is in use or not. Clear the rest of the data.
                record.setId( id );
                record.clear();
                mode.verify( record );
            }
            return record;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    protected void readRecordWithRetry( PageCursor cursor, long id, RECORD record, RecordLoad mode, int offset )
            throws IOException
    {
        // Mark the record with this id regardless of whether or not we load the contents of it.
        // This is done in this method since there are multiple call sites and they all want the id
        // on that record, so it's to ensure it isn't forgotten.
        record.setId( id );

        do
        {
            // Mark this record as unused. This to simplify implementations of readRecord.
            // readRecord can behave differently depending on RecordLoad argument and so it may be that
            // contents of a record may be loaded even if that record is unused, where the contents
            // can still be initialized data. Know that for many record stores, deleting a record means
            // just setting one byte or bit in that record.
            record.setInUse( false );
            cursor.setOffset( offset );
            readRecord( cursor, record, mode );
        }
        while ( cursor.shouldRetry() );
        if ( !mode.verify( record ) )
        {
            record.clear();
        }
    }

    /**
     * Reads data from {@link PageCursor} into the record.
     * @throws IOException on error reading.
     */
    protected abstract void readRecord( PageCursor cursor, RECORD record, RecordLoad mode ) throws IOException;

    @Override
    public void updateRecord( RECORD record )
    {
        long id = record.getId();
        long pageId = pageIdForRecord( id );
        int offset = offsetForId( id );
        try ( PageCursor cursor = storeFile.io( pageId, PF_SHARED_WRITE_LOCK ) )
        {
            if ( cursor.next() )
            {
                do
                {
                    cursor.setOffset( offset );
                    writeRecord( cursor, record );
                }
                while ( cursor.shouldRetry() );
                if ( !record.inUse() )
                {
                    freeId( id );
                }
                if ( (!record.inUse() || !record.requiresSecondaryUnit()) && record.hasSecondaryUnitId() )
                {
                    // If record was just now deleted, or if the record used a secondary unit, but not anymore
                    // then free the id of that secondary unit.
                    freeId( record.getSecondaryUnitId() );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    protected abstract void writeRecord( PageCursor cursor, RECORD record ) throws IOException;

    /**
     * Scan the given range of records both inclusive, and pass all the in-use ones to the given processor, one by one.
     *
     * The record passed to the NodeRecordScanner is reused instead of reallocated for every record, so it must be
     * cloned if you want to save it for later.
     * @param visitor {@link Visitor} notified about all records.
     * @throws IOException on error reading from store.
     */
    public void scanAllRecords( Visitor<RECORD,IOException> visitor ) throws IOException
    {
        long startPageId = pageIdForRecord( 0 );
        long currentPageId = startPageId;
        long endPageId = pageIdForRecord( getHighestPossibleIdInUse() );
        long currentRecordId = 0;
        RECORD record = newRecord();
        int recordsPerPage = storeFile.pageSize() / getRecordSize();

        try ( PageCursor cursor = storeFile.io( startPageId, PF_SHARED_READ_LOCK | PF_READ_AHEAD ) )
        {
            while ( currentPageId <= endPageId && cursor.next() )
            {
                for ( int i = 0; i < recordsPerPage; i++ )
                {
                    if ( getRecord( currentRecordId, record, CHECK, cursor, currentPageId ).inUse() )
                    {
                        if ( visitor.visit( record ) )
                        {
                            return;
                        }
                    }
                    currentRecordId++;
                }
                currentPageId++;
            }
        }
    }

    @Override
    public Collection<RECORD> getRecords( long firstId, RecordLoad mode )
    {
        // TODO we should instead be passed in a consumer of records, so we don't have to spend memory building up
        // this list
        List<RECORD> recordList = new LinkedList<>();
        long currentId = firstId;
        try ( PageCursor cursor = storeFile.io( 0, PF_SHARED_READ_LOCK ) )
        {
            while ( !NULL_REFERENCE.is( currentId ) && cursor.next( pageIdForRecord( currentId ) ) )
            {
                RECORD record = newRecord();
                readRecordWithRetry( cursor, currentId, record, mode, offsetForId( currentId ) );
                if ( !record.inUse() )
                {
                    break;
                }
                recordList.add( record );
                currentId = getNextRecordReference( record );
            }
            return recordList;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public RecordCursor<RECORD> newRecordCursor( final RECORD record )
    {
        return new RecordCursor<RECORD>()
        {
            private long currentId;
            private RecordLoad mode;
            private PageCursor pageCursor;

            @Override
            public boolean next()
            {
                if ( NULL_REFERENCE.is( currentId ) )
                {
                    return false;
                }

                try
                {
                    return getRecord( currentId, record, mode, pageCursor, pageIdForRecord( currentId ) ).inUse();
                }
                finally
                {
                    currentId = record.inUse() ? getNextRecordReference( record ) : NULL_REFERENCE.intValue();
                }
            }

            @Override
            public void close()
            {
                assert pageCursor != null;
                this.pageCursor.close();
                this.pageCursor = null;
            }

            @Override
            public RECORD get()
            {
                return record;
            }

            @Override
            public boolean next( long id )
            {
                currentId = id;
                return next();
            }

            @Override
            public RecordCursor<RECORD> init( long id, RecordLoad mode, PageCursor pageCursor )
            {
                assert this.pageCursor == null;
                this.currentId = id;
                this.mode = mode;
                this.pageCursor = pageCursor;
                return this;
            }
        };
    }

    @Override
    public RecordCursor<RECORD> placeRecordCursor( final long id, final RecordCursor<RECORD> cursor,
            final RecordLoad mode )
    {
        try
        {
            PageCursor pageCursor = storeFile.io( pageIdForRecord( id ), PF_SHARED_READ_LOCK );
            cursor.init( id, mode, pageCursor );
            return cursor;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public void ensureHeavy( RECORD record )
    {
        // Do nothing by default. Some record stores have this.
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    @Override
    public int getStoreHeaderInt()
    {
        throw new UnsupportedOperationException( "No header" );
    }

    public static abstract class Configuration
    {
        public static final Setting<Boolean> rebuild_idgenerators_fast =
                GraphDatabaseSettings.rebuild_idgenerators_fast;
    }
}
