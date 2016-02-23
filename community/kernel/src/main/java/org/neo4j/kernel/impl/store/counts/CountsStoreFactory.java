package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.DatabaseHealth;

public class CountsStoreFactory
{
    private final TransactionIdStore txIdStore;
    private final DatabaseHealth databaseHealth;

    public CountsStoreFactory( TransactionIdStore txIdStore, DatabaseHealth databaseHealth )
    {
        this.txIdStore = txIdStore;
        this.databaseHealth = databaseHealth;
    }

    public CountsStore create( CountsSnapshot snapshot )
    {
        return new InMemoryCountsStore( snapshot, txIdStore, databaseHealth );
    }

    public CountsStore create()
    {
        return new InMemoryCountsStore(txIdStore, databaseHealth );
    }
}
