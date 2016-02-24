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

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexSampleKey;
import org.neo4j.kernel.impl.store.counts.keys.IndexStatisticsKey;
import org.neo4j.kernel.impl.store.counts.keys.NodeKey;
import org.neo4j.kernel.impl.store.counts.keys.RelationshipKey;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class StatisticsRecord extends AbstractBaseRecord
{
    private StatisticsEntry entry;

    public StatisticsRecord( long id )
    {
        super( id );
    }

    public StatisticsRecord( long id, CountsKey key, long[] value )
    {
        super( id );
        switch ( key.recordType() )
        {
        case ENTITY_NODE:
            NodeKey nodeKey = (NodeKey) key;
            this.entry = new NodeWithLabelCount(
                    nodeKey.getLabelId(),
                    value[0] );
            break;
        case ENTITY_RELATIONSHIP:
            RelationshipKey relationshipKey = (RelationshipKey) key;
            this.entry = new RelationshipWithLabelsCount(
                    relationshipKey.getStartLabelId(),
                    relationshipKey.getTypeId(),
                    relationshipKey.getEndLabelId(),
                    value[0] );
            break;
        case INDEX_SAMPLE:
            IndexSampleKey indexSampleKey = (IndexSampleKey) key;
            this.entry = new IndexSampleWithCount(
                    indexSampleKey.labelId(),
                    indexSampleKey.propertyKeyId(),
                    value[0],
                    value[1] );
            break;
        case INDEX_STATISTICS:
            IndexStatisticsKey indexStatisticsKey = (IndexStatisticsKey) key;
            this.entry = new IndexSampleWithCount(
                    indexStatisticsKey.labelId(),
                    indexStatisticsKey.propertyKeyId(),
                    value[0],
                    value[1] );
            break;
        default:
            throw new IllegalArgumentException();
        }
    }

    public StatisticsEntry getEntry()
    {
        return entry;
    }

    public void setEntry( StatisticsEntry entry )
    {
        this.entry = entry;
    }

    @Override
    public String toString()
    {
        return "StatisticsRecord{" +
               "inUse=" + inUse() +
               ", entry=" + entry +
               '}';
    }
}
