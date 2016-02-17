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

import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.register.Register;

public class ReadOnlyCountsStorageService implements CountsStorageService
{
    private final CountsStorageServiceImpl delegate;

    public ReadOnlyCountsStorageService( CountsStorageServiceImpl delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public Updater updaterFor( long txId )
    {
        return null;
    }

    @Override
    public IndexStatsUpdater indexStatsUpdater()
    {
        return null;
    }

    @Override
    public Updater apply( long txId )
    {
        return null;
    }

    @Override
    public CountsSnapshot snapshot( long txId )
    {
        return delegate.snapshot( txId );
    }

    @Override
    public Register.DoubleLongRegister nodeCount( int labelId, Register.DoubleLongRegister target )
    {
        return delegate.nodeCount( labelId, target );
    }

    @Override
    public Register.DoubleLongRegister relationshipCount( int startLabelId, int typeId, int endLabelId,
            Register.DoubleLongRegister target )
    {
        return null;
    }

    @Override
    public Register.DoubleLongRegister indexUpdatesAndSize( int labelId, int propertyKeyId,
            Register.DoubleLongRegister target )
    {
        return null;
    }

    @Override
    public Register.DoubleLongRegister indexSample( int labelId, int propertyKeyId, Register.DoubleLongRegister target )
    {
        return null;
    }

    @Override
    public void accept( CountsVisitor visitor )
    {

    }

    @Override
    public void initialize( CountsSnapshot snapshot )
    {
        delegate.initialize( snapshot );
    }
}
