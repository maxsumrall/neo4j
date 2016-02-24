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
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyType;

public class IndexSampleWithCount extends StatisticsEntry
{
    private final int labelId;
    private final int propertyKeyId;
    private final long long1;
    private final long long2;

    public IndexSampleWithCount( int labelId, int propertyKeyId, long long1, long long2 )
    {
        super( CountsKeyType.INDEX_SAMPLE);
        this.labelId = labelId;
        this.propertyKeyId = propertyKeyId;
        this.long1 = long1;
        this.long2 = long2;
    }

    @Override
    public Pair<CountsKey,long[]> asSnapshotEntry()
    {
        return null;
    }

    public int getLabelId()
    {
        return labelId;
    }

    public int getPropertyKeyId()
    {
        return propertyKeyId;
    }

    public long getLong1()
    {
        return long1;
    }

    public long getLong2()
    {
        return long2;
    }
}
