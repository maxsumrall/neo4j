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
package org.neo4j.kernel.impl.store.record.statistics;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;

public class NodeWithLabelCount extends StatisticsEntry
{

    private final int labelId;
    private final long count;

    public NodeWithLabelCount( int labelId, long count )
    {
        super( CountsKeyType.ENTITY_NODE );
        this.labelId = labelId;
        this.count = count;
    }

    public int labelId()
    {
        return labelId;
    }

    public long count()
    {
        return count;
    }

    @Override
    public Pair<CountsKey,long[]> asSnapshotEntry()
    {
        return Pair.of( CountsKeyFactory.nodeKey( labelId ), new long[]{count} );
    }

    @Override
    public String toString()
    {
        return "NodeWithLabelCount{" +
               "labelId=" + labelId +
               ", count=" + count +
               '}';
    }
}
