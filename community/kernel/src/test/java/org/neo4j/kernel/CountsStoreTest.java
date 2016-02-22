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
