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
package org.neo4j.kernel;

import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

// todo: should not be committed
public class CountsStoreTest
{
    private static final Label PERSON = Label.label( "Person" );
    private static final String NAME = "name";

    @Test
    public void name() throws Exception
    {
        File storeDir = new File( "/tmp/testdb" );
//        FileUtils.deleteRecursively( storeDir );
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.pagecache_memory, "20m" )
                .newGraphDatabase();
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( PERSON );
                node.setProperty( NAME, "apa" );
                tx.success();
            }
        }
        finally
        {
            db.shutdown();
        }
    }
}
