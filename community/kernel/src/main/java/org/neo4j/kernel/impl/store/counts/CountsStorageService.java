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
package org.neo4j.kernel.impl.store.counts;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StatisticsStore;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.register.Register;

public class CountsStorageService implements CountsAccessor, CountsVisitor.Visitable, Lifecycle
{
    private final CountsTracker countsTracker;
    private CountsStore countsStore;
    private final StatisticsStore statisticsStore;
    private final TransactionIdStore txIdStore;
    private final CountsComputer countsComputer;
    private final DatabaseHealth databaseHealth;

    private boolean needsRebuild;

    public CountsStorageService( NeoStores neoStores, DatabaseHealth databaseHealth )
    {
        this.countsTracker = neoStores.getCounts();
        this.statisticsStore = neoStores.getStatisticsStore();
        this.txIdStore = neoStores.getMetaDataStore();
        this.countsComputer = new CountsComputer( neoStores );
        this.databaseHealth = databaseHealth;
    }

    @Override
    public void init() throws Throwable
    {
        CountsSnapshot snapshot = statisticsStore.read();

        if ( snapshot == null )
        {
            needsRebuild = true;
            countsStore = new DummyCountsStore();
        }
        else
        {
            countsStore = new InMemoryCountsStore( snapshot, databaseHealth );
        }
    }

    @Override
    public void start() throws Throwable
    {
        if ( needsRebuild )
        {
            long lastCommittedTransactionId = txIdStore.getLastCommittedTransactionId();
            countsStore = new InMemoryCountsStore( lastCommittedTransactionId, databaseHealth );
            try ( Updater updater = updaterFor( lastCommittedTransactionId ) )
            {
                countsComputer.initialize( updater );
            }
            needsRebuild = false;
        }
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    public void force()
    {
        if ( needsRebuild )
        {
            return;
        }
        try
        {
            long lastCommittedTxId = txIdStore.getLastCommittedTransactionId();
            CountsSnapshot snapshot = countsStore.snapshot( lastCommittedTxId );
            statisticsStore.write( snapshot );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public Updater updaterFor( long txId )
    {
        Updater countsTrackerUpdater = countsTracker.apply( txId ).orElse( null );
        CountsStoreUpdater countsStoreUpdater = new CountsStoreUpdater( txId, countsStore );
        if ( countsTrackerUpdater == null )
        {
            return countsStoreUpdater;
        }
        return new MultipleCountsUpdater( new Updater[]{countsTrackerUpdater, countsStoreUpdater} );
    }

    public Updater apply( long txId )
    {
        return updaterFor( txId );
    }

    @Override
    public Register.DoubleLongRegister nodeCount( int labelId, Register.DoubleLongRegister target )
    {
        countsTracker.nodeCount( labelId, target );
        long value = countsStore.get( CountsKeyFactory.nodeKey( labelId ) )[0];
        if ( target.readSecond() != value )
        {
            throw new AssertionError(
                    "Values from countsTracker and countsStore do not match: " + target.readSecond() + " and " +
                    value );
        }
        target.write( 0, value );
        return target;
    }

    @Override
    public Register.DoubleLongRegister relationshipCount( int startLabelId, int typeId, int endLabelId,
            Register.DoubleLongRegister target )
    {
        countsTracker.relationshipCount( startLabelId, typeId, endLabelId, target );
        long value = countsStore.get( CountsKeyFactory.relationshipKey( startLabelId, typeId, endLabelId ) )[0];
        if ( target.readSecond() != value )
        {
            throw new AssertionError(
                    "Values from countsTracker and countsStore do not match: " + target.readSecond() + " and " +
                    value );
        }
        target.write( 0, value );
        return target;
    }

    @Override
    public Register.DoubleLongRegister indexUpdatesAndSize( int labelId, int propertyKeyId,
            Register.DoubleLongRegister target )
    {
        countsTracker.indexUpdatesAndSize( labelId, propertyKeyId, target );
        long[] values = countsStore.get( CountsKeyFactory.indexStatisticsKey( labelId, propertyKeyId ) );
        if ( target.readFirst() != values[0] || target.readSecond() != values[1] )
        {
            throw new AssertionError(
                    "Values from countsTracker and countsStore do not match: " + target.readFirst() + " and " +
                    values[0] + " and " + target.readSecond() + " and " + values[1] );
        }
        target.write( values[0], values[1] );
        return target;
    }

    @Override
    public Register.DoubleLongRegister indexSample( int labelId, int propertyKeyId, Register.DoubleLongRegister target )
    {
        countsTracker.indexSample( labelId, propertyKeyId, target );
        long[] values = countsStore.get( CountsKeyFactory.indexSampleKey( labelId, propertyKeyId ) );
        if ( target.readFirst() != values[0] || target.readSecond() != values[1] )
        {
            throw new AssertionError(
                    "Values from countsTracker and countsStore do not match: " + target.readFirst() + " and " +
                    values[0] + " and " + target.readSecond() + " and " + values[1] );
        }
        target.write( values[0], values[1] );
        return target;
    }

    @Override
    public void accept( CountsVisitor visitor )
    {
        countsStore.forEach( ( countsKey, values ) -> {
            switch ( countsKey.recordType() )
            {
            case ENTITY_NODE:
            {
                visitor.visitNodeCount( ((NodeKey) countsKey).getLabelId(), values[0] );
                break;
            }
            case ENTITY_RELATIONSHIP:
            {
                RelationshipKey k = (RelationshipKey) countsKey;
                visitor.visitRelationshipCount( k.getStartLabelId(), k.getTypeId(), k.getEndLabelId(), values[0] );
                break;
            }
            case INDEX_STATISTICS:
            {
                IndexStatisticsKey key = (IndexStatisticsKey) countsKey;
                visitor.visitIndexStatistics( key.labelId(), key.propertyKeyId(), values[0], values[1] );
                break;
            }
            case INDEX_SAMPLE:
            {
                IndexSampleKey key = (IndexSampleKey) countsKey;
                visitor.visitIndexSample( key.labelId(), key.propertyKeyId(), values[0], values[1] );
                break;
            }
            default:
                throw new IllegalStateException( "unexpected counts key " + countsKey );
            }
        } );
    }
}
