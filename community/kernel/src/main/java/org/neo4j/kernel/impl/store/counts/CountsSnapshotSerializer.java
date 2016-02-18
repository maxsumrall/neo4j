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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_NODE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.ENTITY_RELATIONSHIP;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_SAMPLE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyType.INDEX_STATISTICS;

public class CountsSnapshotSerializer
{
    public static void serializeHeader( PageCursor cursor, long txId, int size )
    {
        cursor.putLong( txId );
        cursor.putInt( size );
    }

    public static void serialize( PageCursor cursor, CountsKey key, long[] value ) throws IOException
    {
        switch ( key.recordType() )
        {

        case ENTITY_NODE:
            if ( value.length != 1 )
            {
                throw new IllegalArgumentException(
                        "CountsKey of type " + key.recordType() + " has an unexpected value." );
            }
            NodeKey nodeKey = (NodeKey) key;
            cursor.putByte( ENTITY_NODE.code );
            cursor.putInt( nodeKey.getLabelId() );
            cursor.putLong( value[0] );
            break;

        case ENTITY_RELATIONSHIP:
            if ( value.length != 1 )
            {
                throw new IllegalArgumentException(
                        "CountsKey of type " + key.recordType() + " has an unexpected value." );
            }
            RelationshipKey relationshipKey = (RelationshipKey) key;
            cursor.putByte( ENTITY_RELATIONSHIP.code );
            cursor.putInt( relationshipKey.getStartLabelId() );
            cursor.putInt( relationshipKey.getTypeId() );
            cursor.putInt( relationshipKey.getEndLabelId() );
            cursor.putLong( value[0] );
            break;

        case INDEX_SAMPLE:
            if ( value.length != 2 )
            {
                throw new IllegalArgumentException(
                        "CountsKey of type " + key.recordType() + " has an unexpected value." );
            }
            IndexSampleKey indexSampleKey = (IndexSampleKey) key;
            cursor.putByte( INDEX_SAMPLE.code );
            cursor.putInt( indexSampleKey.labelId() );
            cursor.putInt( indexSampleKey.propertyKeyId() );
            cursor.putLong( value[0] );
            cursor.putLong( value[1] );
            break;

        case INDEX_STATISTICS:
            if ( value.length != 2 )
            {
                throw new IllegalArgumentException(
                        "CountsKey of type " + key.recordType() + " has an unexpected value." );
            }
            IndexStatisticsKey indexStatisticsKey = (IndexStatisticsKey) key;
            cursor.putByte( INDEX_STATISTICS.code );
            cursor.putInt( indexStatisticsKey.labelId() );
            cursor.putInt( indexStatisticsKey.propertyKeyId() );
            cursor.putLong( value[0] );
            cursor.putLong( value[1] );
            break;

        case EMPTY:
            throw new IllegalArgumentException( "CountsKey of type EMPTY cannot be serialized." );

        default:
            throw new IllegalArgumentException( "The read CountsKey has an unknown type." );

        }
    }
}
