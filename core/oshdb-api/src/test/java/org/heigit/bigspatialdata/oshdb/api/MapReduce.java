/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class MapReduce {
  private final OSHDB oshdb;

  private final BoundingBox bbox = new BoundingBox(8, 9, 49, 50);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps(2010, 2015, 1, 12);

  private final double DELTA = 1e-8;

  public MapReduce() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data;ACCESS_MODE_DATA=r");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmTypes(OSMType.NODE).filterByTagKey("highway").areaOfInterest(bbox);
  }

  @Test
  public void test() throws Exception {
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter(entity -> entity.getId() == 617308093)
        .mapReduce(
            OSMContribution::getContributorUserId,
            HashSet::new,
            (x,y) -> { x.add(y); return x; },
            (x,y) -> { HashSet<Integer> ret = new HashSet<>(x); ret.addAll(y); return ret; }
        );

    /* should be 5: first version doesn't have the highway tag, remaining 7 versions have 5 different contributor user ids*/
    assertEquals(5, result.size());
  }
}
