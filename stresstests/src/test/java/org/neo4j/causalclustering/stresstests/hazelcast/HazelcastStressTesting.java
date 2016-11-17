package org.neo4j.causalclustering.stresstests.hazelcast;

import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.ListenSocketAddress;

import static java.util.Arrays.asList;

public class HazelcastStressTesting
{
    public static void main( String[] args )
    {
        if ( args.length < 2 )
        {
            System.out.println( "Usage java HazelcastStressTesting <this instance's address> <other cluster members " +
                    "address>..." );
            System.exit( 1 );
        }
        new HazelcastStressTesting().createHazelcastInstance( args[0], asList( args ).subList( 1, args.length ) );
        //completed, everything must be fine.
    }

    @Test
    public void pleaseFail() throws Exception
    {
        ExecutorService executorService = Executors.newFixedThreadPool( 4 );
        // given
        List<String> members = asList( "127.0.0.1:7900", "127.0.0.1:7901", "127.0.0.1:7902" );
        for ( String memberAddress : members )
        {
            executorService.execute( () -> createHazelcastInstance( memberAddress, members ) );
        }

        executorService.execute( () -> createHazelcastInstance( "127.0.0.1:7903", members ) );

        // when
        executorService.awaitTermination( 5, TimeUnit.MINUTES );
        // then
        Hazelcast.shutdownAll();
    }

    @Test
    public void doItInANewProcess() throws Exception
    {
        // given
        List<String> members = asList( "127.0.0.1:7900", "127.0.0.1:7901", "127.0.0.1:7902" );
        for ( String memberAddress : members )
        {
            Process process = new ProcessBuilder(
                    "java", "/Users/Max/neo4j/neo4j3.1/stresstests/target/test-classes/org/neo4j/causalclustering" +
                            "/stresstests" +
                            "/hazelcast/HazelcastStressTesting.class", memberAddress,
                    Arrays.toString( members.toArray() ) ).inheritIO().start();
        }
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
        c.setProperty( GroupProperties.PROP_INITIAL_MIN_CLUSTER_SIZE, String.valueOf( (members.size() / 2) + 1 ) );
        c.setProperty( GroupProperties.PROP_LOGGING_TYPE, "none" );

        c.setNetworkConfig( networkConfig );


        return Hazelcast.newHazelcastInstance( c );
    }
}
