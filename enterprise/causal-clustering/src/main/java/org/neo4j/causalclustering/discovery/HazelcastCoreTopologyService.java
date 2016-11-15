/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.discovery;

import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.GroupProperties;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.hazelcast.spi.properties.GroupProperty.INITIAL_MIN_CLUSTER_SIZE;
import static com.hazelcast.spi.properties.GroupProperty.LOGGING_TYPE;
import static com.hazelcast.spi.properties.GroupProperty.MAX_JOIN_MERGE_TARGET_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_JOIN_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.GroupProperty.WAIT_SECONDS_BEFORE_JOIN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.discovery_listen_address;
import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

class HazelcastCoreTopologyService extends LifecycleAdapter implements CoreTopologyService
{
    private final Config config;
    private final MemberId myself;
    private final Log log;
    private final Log userLog;
    private final CoreTopologyListenerService listenerService;
    private final JobScheduler scheduler;
    private String membershipRegistrationId;

    private JobScheduler.JobHandle jobHandle;

    private HazelcastInstance hazelcastInstance;
    private volatile ReadReplicaTopology latestReadReplicaTopology;
    private volatile CoreTopology latestCoreTopology;

    HazelcastCoreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler, LogProvider logProvider,
            LogProvider userLogProvider )
    {
        this.config = config;
        this.myself = myself;
        this.scheduler = jobScheduler;
        this.listenerService = new CoreTopologyListenerService();
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
    }

    @Override
    public void addCoreTopologyListener( Listener listener )
    {
        listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange( coreServers() );
    }

    @Override
    public boolean setClusterId( ClusterId clusterId )
    {
        return HazelcastClusterTopology.casClusterId( hazelcastInstance, clusterId );
    }

    @Override
    public void start()
    {
        hazelcastInstance = createHazelcastInstanceWithRetries();
        log.info( "Cluster discovery service started" );
        membershipRegistrationId = hazelcastInstance.getCluster().addMembershipListener( new OurMembershipListener() );
        refreshCoreTopology();
        refreshReadReplicaTopology();
        listenerService.notifyListeners( coreServers() );

        try
        {
            scheduler.start();
        }
        catch ( Throwable throwable )
        {
            log.debug( "Failed to start job scheduler." );
            return;
        }

        JobScheduler.Group group = new JobScheduler.Group( "Scheduler", POOLED );
        jobHandle = this.scheduler.scheduleRecurring( group, () ->
        {
            refreshCoreTopology();
            refreshReadReplicaTopology();
        }, config.get( CausalClusteringSettings.cluster_topology_refresh ), MILLISECONDS );
    }

    @Override
    public void stop()
    {
        log.info( String.format( "HazelcastCoreTopologyService stopping and unbinding from %s",
                config.get( discovery_listen_address ) ) );
        try
        {
            hazelcastInstance.getCluster().removeMembershipListener( membershipRegistrationId );
            hazelcastInstance.getLifecycleService().terminate();
        }
        catch ( Throwable e )
        {
            log.warn( "Failed to stop Hazelcast", e );
        }
        finally
        {
            jobHandle.cancel( true );
        }
    }

    private HazelcastInstance createHazelcastInstanceWithRetries()
    {
        CompletableFuture<HazelcastInstance> hazelcastInstanceFuture = new CompletableFuture<>();

        HazelcastCreator hazelcastCreator = new HazelcastCreator( 5, hazelcastInstanceFuture );

        JobScheduler.JobHandle jobHandle = scheduler
                .schedule( new JobScheduler.Group( getClass().toString(), POOLED ), hazelcastCreator, 100,
                        MILLISECONDS );
        hazelcastInstanceFuture.whenComplete( ( result, e ) -> jobHandle.cancel( true ) );

        try
        {
            return hazelcastInstanceFuture.get();
        }
        catch ( Exception e )
        {
            String errorMessage = String.format( "Hazelcast was unable to start with setting: %s = %s",
                    discovery_listen_address.name(), config.get( discovery_listen_address ) );
            userLog.error( errorMessage );
            log.error( errorMessage, e );
            throw new RuntimeException( e );
        }
    }

    private class HazelcastCreator implements Runnable
    {
        private final int retries;
        private final CompletableFuture<HazelcastInstance> hazelcastInstanceFuture;

        public HazelcastCreator( int retries, CompletableFuture<HazelcastInstance> hazelcastInstanceFuture )
        {
            this.retries = retries;
            this.hazelcastInstanceFuture = hazelcastInstanceFuture;
        }

        @Override
        public void run()
        {
            try
            {
                Hazelcast.shutdownAll(); //Make sure we start with a clean slate.
            }
            catch ( Exception e )
            {
                //meh
            }

            try
            {
                HazelcastInstance hazelcastInstance = createHazelcastInstance();
                hazelcastInstanceFuture.complete( hazelcastInstance );
            }
            catch ( Exception e )
            {
                if ( retries > 0 )
                {
                    scheduler.schedule( new JobScheduler.Group( getClass().toString(), POOLED ),
                            new HazelcastCreator( retries - 1, hazelcastInstanceFuture ), 100, MILLISECONDS );
                }
                else
                {
                    hazelcastInstanceFuture.completeExceptionally(
                            new Throwable( "Failed to create a working " + "Hazelcast instance." ) );
                }
            }
        }
    }

    private HazelcastInstance createHazelcastInstance()
    {
        System.setProperty( GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1" );

        JoinConfig joinConfig = new JoinConfig();
        joinConfig.getMulticastConfig().setEnabled( false );
        joinConfig.getAwsConfig().setEnabled( false );
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        tcpIpConfig.setEnabled( true );

        List<AdvertisedSocketAddress> initialMembers = config.get( CausalClusteringSettings.initial_discovery_members );
        for ( AdvertisedSocketAddress address : initialMembers )
        {
            tcpIpConfig.addMember( address.toString() );
        }
        log.info( "Discovering cluster with initial members: " + initialMembers );

        NetworkConfig networkConfig = new NetworkConfig();
        Setting<ListenSocketAddress> discovery_listen_address = CausalClusteringSettings.discovery_listen_address;
        ListenSocketAddress hazelcastAddress = config.get( discovery_listen_address );
        InterfacesConfig interfaces = new InterfacesConfig();
        interfaces.addInterface( hazelcastAddress.getHostname() );
        networkConfig.setInterfaces( interfaces );
        networkConfig.setPort( hazelcastAddress.getPort() );
        networkConfig.setJoin( joinConfig );
        networkConfig.setPortAutoIncrement( false );
        networkConfig.setReuseAddress( true );
        com.hazelcast.config.Config c = new com.hazelcast.config.Config();
        c.setProperty( OPERATION_CALL_TIMEOUT_MILLIS.getName(), String.valueOf( 10_000 ) );
        c.setProperty( INITIAL_MIN_CLUSTER_SIZE.getName(),
                String.valueOf( minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize() ) );
        c.setProperty( LOGGING_TYPE.getName(), "none" );
        c.setProperty( MAX_JOIN_SECONDS.getName(), "60" );
        c.setProperty( MAX_JOIN_MERGE_TARGET_SECONDS.getName(), "5" );
        c.setProperty( WAIT_SECONDS_BEFORE_JOIN.getName(), "1" );

        c.setNetworkConfig( networkConfig );

        MemberAttributeConfig memberAttributeConfig = HazelcastClusterTopology.buildMemberAttributes( myself, config );

        c.setMemberAttributeConfig( memberAttributeConfig );
        userLog.info( "Waiting for other members to join cluster before continuing..." );

        return Hazelcast.newHazelcastInstance( c );
    }

    private Integer minimumClusterSizeThatCanTolerateOneFaultForExpectedClusterSize()
    {
        return (config.get( CausalClusteringSettings.expected_core_cluster_size ) / 2) + 1;
    }

    @Override
    public ReadReplicaTopology readReplicas()
    {
        return latestReadReplicaTopology;
    }

    @Override
    public CoreTopology coreServers()
    {
        return latestCoreTopology;
    }

    @Override
    public void refreshCoreTopology()
    {
        latestCoreTopology = HazelcastClusterTopology.getCoreTopology( hazelcastInstance, log );
        log.info( "Current core topology is %s", coreServers() );
        listenerService.notifyListeners( coreServers() );
    }

    private void refreshReadReplicaTopology()
    {
        latestReadReplicaTopology = HazelcastClusterTopology.getReadReplicaTopology( hazelcastInstance, log );
        log.info( "Current read replica topology is %s", latestReadReplicaTopology );
    }

    private class OurMembershipListener implements MembershipListener
    {
        @Override
        public void memberAdded( MembershipEvent membershipEvent )
        {
            log.info( "Core member added %s", membershipEvent );
            refreshCoreTopology();
        }

        @Override
        public void memberRemoved( MembershipEvent membershipEvent )
        {
            log.info( "Core member removed %s", membershipEvent );
            refreshCoreTopology();
        }

        @Override
        public void memberAttributeChanged( MemberAttributeEvent memberAttributeEvent )
        {
        }
    }
}
