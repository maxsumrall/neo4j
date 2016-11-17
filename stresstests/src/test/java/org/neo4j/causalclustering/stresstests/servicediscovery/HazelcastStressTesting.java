package org.neo4j.causalclustering.stresstests.servicediscovery;

import com.hazelcast.core.Hazelcast;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HazelcastStressTesting
{
    /**
     * Starts a cluster of HC members, which are configured to know the other cluster members on start up except for one
     * instance. After they start, the last and unknown instance is started. This tests that the unknown instance is
     * able to join the cluster even when it is not known by the other members.
     *
     * @throws Exception
     */
    @Test
    public void scenarioA() throws Exception
    {
        ExecutorService executorService = Executors.newCachedThreadPool();
        DiscoveryService.HazelcastDiscoveryService hazelcastDiscoveryService =
                new DiscoveryService.HazelcastDiscoveryService( executorService );
        // given
        List<String> members = asList( "127.0.0.1:7900", "127.0.0.1:7901", "127.0.0.1:7902" );
        CountDownLatch latch = new CountDownLatch( members.size() );
        for ( String bindAddress : members )
        {
            hazelcastDiscoveryService.getService( bindAddress, members, latch );
        }

        //wait for them to be up.
        latch.await( 5, TimeUnit.MINUTES );

        String address = "127.0.0.1:7903";
        Future<DiscoveryService> service =
                hazelcastDiscoveryService.getService( address, members.subList( 0, 2 ), latch );

        // when
        executorService.shutdown();

        try
        {
            service.get( 5, TimeUnit.MINUTES );
        }
        catch ( TimeoutException e )
        {
            fail( "Did not create discovery service within allowed time." );
        }
        // then
        Hazelcast.shutdownAll();
    }

    /**
     * Starts one HC instance which knows about some of the other members.
     * Waits, then starts the other HC members who do not know about the first member.
     * This tests the discovery service on the first machine and its ability to join the cluster even when
     * it is not known by other members.
     *
     * @throws Exception
     */
    @Test
    public void scenarioB() throws Exception
    {
        ExecutorService executorService = Executors.newCachedThreadPool();
        DiscoveryService.HazelcastDiscoveryService hazelcastDiscoveryService =
                new DiscoveryService.HazelcastDiscoveryService( executorService );
        List<String> members = asList( "127.0.0.1:7900", "127.0.0.1:7901", "127.0.0.1:7902" );
        final AtomicLong startTime = new AtomicLong();

        // given
        String address = "127.0.0.1:7903";

        startTime.set( System.currentTimeMillis() );
        Future<DiscoveryService> misconfiguredMember =
                hazelcastDiscoveryService.getService( address, members.subList( 0, 2 ) );

        Thread.sleep( TimeUnit.MINUTES.toMillis( 1 ) );

        for ( String bindAddress : members )
        {
            hazelcastDiscoveryService.getService( bindAddress, members );
        }

        misconfiguredMember.get( 5, TimeUnit.MINUTES );

        // when
        executorService.shutdown();
        assertTrue( executorService.awaitTermination( 5, TimeUnit.MINUTES ) );
        // then
        Hazelcast.shutdownAll();

    }
}
