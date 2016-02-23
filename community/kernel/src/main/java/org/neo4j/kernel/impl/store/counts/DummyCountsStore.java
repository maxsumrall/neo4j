package org.neo4j.kernel.impl.store.counts;

import java.util.Map;
import java.util.function.BiConsumer;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;

public class DummyCountsStore implements CountsStore
{
    @Override
    public long[] get( CountsKey key )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAll( long txId, Map<CountsKey,long[]> deltas )
    {
    }

    @Override
    public void update( CountsKey key, long[] delta )
    {
    }

    @Override
    public void replace( CountsKey key, long[] replacement )
    {
    }

    @Override
    public CountsSnapshot snapshot( long txId )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CountsSnapshot snapshot()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEach( BiConsumer<CountsKey,long[]> action )
    {
    }

    @Override
    public boolean seenTx( long txId )
    {
        return false;
    }
}
