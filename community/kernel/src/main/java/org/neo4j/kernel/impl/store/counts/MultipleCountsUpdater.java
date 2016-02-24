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

import org.neo4j.kernel.impl.api.CountsAccessor;

public class MultipleCountsUpdater implements CountsAccessor.Updater
{
    private final CountsAccessor.Updater[] updaters;

    public MultipleCountsUpdater( CountsAccessor.Updater[] updaters )
    {
        this.updaters = updaters;
    }

    @Override
    public void incrementNodeCount( int labelId, long delta )
    {
        for ( CountsAccessor.Updater updater : updaters )
        {
            updater.incrementNodeCount( labelId, delta );
        }
    }

    @Override
    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        for ( CountsAccessor.Updater updater : updaters )
        {
            updater.incrementRelationshipCount( startLabelId, typeId, endLabelId, delta );
        }
    }

    @Override
    public void close()
    {
        for ( CountsAccessor.Updater updater : updaters )
        {
            updater.close();
        }
    }
}
