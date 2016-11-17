package org.neo4j.causalclustering.stresstests.servicediscovery;

import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.neo4j.helpers.ListenSocketAddress;

public interface DiscoveryService
{
    Future<DiscoveryService> getService( String bindAddress, List<String> clusterMembers, CountDownLatch latch );

    Future<DiscoveryService> getService( String bindAddress, List<String> clusterMembers );


    class HazelcastDiscoveryService implements DiscoveryService
    {
        private final ExecutorService executorService;
        HazelcastInstance hazelcastInstance;
        HazelcastDiscoveryService( ExecutorService executorService )
        {

            this.executorService = executorService;
        }

        @Override
        public Future<DiscoveryService> getService( String bindAddress, List<String> clusterMembers,
                CountDownLatch latch )
        {
            return executorService.submit( () ->
            {
                hazelcastInstance = createHazelcastInstance( bindAddress, clusterMembers );
                System.out.println( "HC instance at " + bindAddress + " created." );
                latch.countDown();
                return this;
            } );
        }

        @Override
        public Future<DiscoveryService> getService( String bindAddress, List<String> clusterMembers )
        {
            return getService( bindAddress, clusterMembers, new CountDownLatch(0) );
        }

        private HazelcastInstance createHazelcastInstance( String memberAddress, List<String> members )
        {
            JoinConfig joinConfig = new JoinConfig();
            joinConfig.getMulticastConfig().setEnabled( false );
            TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
            tcpIpConfig.setEnabled( true );
            tcpIpConfig.setMembers( members );

            NetworkConfig networkConfig = new NetworkConfig();
            ListenSocketAddress hazelcastAddress = new ListenSocketAddress( memberAddress.split( ":" )[0],
                    Integer.valueOf( memberAddress.split( ":" )[1] ) );
            InterfacesConfig interfaces = new InterfacesConfig();
            interfaces.addInterface( hazelcastAddress.getHostname() );
            networkConfig.setInterfaces( interfaces );
            networkConfig.setPort( hazelcastAddress.getPort() );
            networkConfig.setJoin( joinConfig );
            networkConfig.setPortAutoIncrement( false );
            com.hazelcast.config.Config c = new com.hazelcast.config.Config();
            c.setProperty( GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS, "10000" );
            c.setProperty( GroupProperty.CONNECT_ALL_WAIT_SECONDS, "5" );
            c.setProperty( GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS, "15" );
            c.setProperty( GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS, "15" );
            c.setProperty( GroupProperties.PROP_INITIAL_MIN_CLUSTER_SIZE, String.valueOf( (members.size() / 2) + 1 ) );
            c.setProperty( "com.hazelcast.logger.level", "fine" );
            c.setNetworkConfig( networkConfig );

            return Hazelcast.newHazelcastInstance( c );
        }
    }

}
