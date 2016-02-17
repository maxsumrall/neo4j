package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.impl.store.counts.CountsStorageService;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

public class DualCountsAccessor implements CountsAccessor
{
    private CountsTracker countsTracker;
    private CountsStorageService countsStorageService;

    public DualCountsAccessor( CountsTracker countsTracker, CountsStorageService countsStorageService )
    {
        this.countsTracker = countsTracker;
        this.countsStorageService = countsStorageService;
    }

    @Override
    public Register.DoubleLongRegister nodeCount( int labelId, Register.DoubleLongRegister target )
    {

        Register.DoubleLongRegister oldCS = countsTracker.nodeCount( labelId, target );
        Register.DoubleLongRegister newCS =
                countsStorageService.nodeCount( labelId, Registers.newDoubleLongRegister() );
        assert (Long.compare( oldCS.readFirst(), newCS.readFirst() ) == 0);
        return oldCS;
    }

    @Override
    public Register.DoubleLongRegister relationshipCount( int startLabelId, int typeId, int endLabelId,
            Register.DoubleLongRegister target )
    {
        Register.DoubleLongRegister oldCS = countsTracker.relationshipCount( startLabelId, typeId, endLabelId, target );
        Register.DoubleLongRegister newCS = countsStorageService
                .relationshipCount( startLabelId, typeId, endLabelId, Registers.newDoubleLongRegister() );
        assert (Long.compare( oldCS.readFirst(), newCS.readFirst() ) == 0);
        return oldCS;
    }

    @Override
    public Register.DoubleLongRegister indexUpdatesAndSize( int labelId, int propertyKeyId,
            Register.DoubleLongRegister target )
    {
        Register.DoubleLongRegister oldCS = countsTracker.indexUpdatesAndSize( labelId, propertyKeyId, target );
        Register.DoubleLongRegister newCS =
                countsStorageService.indexUpdatesAndSize( labelId, propertyKeyId, Registers.newDoubleLongRegister() );
        assert (Long.compare( oldCS.readFirst(), newCS.readFirst() ) == 0);
        assert (Long.compare( oldCS.readSecond(), newCS.readSecond() ) == 0);
        return oldCS;
    }

    @Override
    public Register.DoubleLongRegister indexSample( int labelId, int propertyKeyId, Register.DoubleLongRegister target )
    {
        Register.DoubleLongRegister oldCS = countsTracker.indexSample( labelId, propertyKeyId, target );
        Register.DoubleLongRegister newCS =
                countsStorageService.indexSample( labelId, propertyKeyId, Registers.newDoubleLongRegister() );
        assert (Long.compare( oldCS.readFirst(), newCS.readFirst() ) == 0)
                : "Old " + oldCS.readFirst() + " new " + newCS.readFirst();
        assert (Long.compare( oldCS.readSecond(), newCS.readSecond() ) == 0)
                : "Old " + oldCS.readSecond() + " new " + newCS.readSecond();
        return oldCS;
    }

    @Override
    public void accept( CountsVisitor visitor )
    {
        countsTracker.accept( visitor );
        countsStorageService.accept( visitor );
    }
}
