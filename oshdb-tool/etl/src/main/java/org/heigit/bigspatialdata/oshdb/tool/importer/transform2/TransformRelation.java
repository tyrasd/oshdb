package org.heigit.bigspatialdata.oshdb.tool.importer.transform2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform2.Transform.Args;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.cellmapping.CellDataSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.cellmapping.CellRefSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSource;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Relation;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.RelationMember;

import com.google.common.base.Stopwatch;


import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class TransformRelation extends Transformer {
	
	private final CellDataSink nonSimpleRelSink;
	private final IdToCellSource nodeToCell;
	private final IdToCellSource wayToCell;
	private final CellRefSink cellRefMap;
	

	private final ByteArrayOutputWrapper baData = new ByteArrayOutputWrapper(1024);
	private final ByteArrayOutputWrapper baRecord = new ByteArrayOutputWrapper(1024);
	private final ByteArrayOutputWrapper baAux = new ByteArrayOutputWrapper(1024);
	
	public TransformRelation(TagToIdMapper tagToId, RoleToIdMapper roleToId, CellDataSink cellDataMap, CellDataSink nonSimpleRelSink, IdToCellSink idToCellSink, IdToCellSource nodeToCell, IdToCellSource wayToCell, CellRefSink cellRefMap) {
			super(tagToId, roleToId, cellDataMap, idToCellSink);
		this.nonSimpleRelSink = nonSimpleRelSink;
		this.nodeToCell = nodeToCell;
		this.wayToCell = wayToCell;
		this.cellRefMap = cellRefMap;
	}
	
	private OSMRelation getOSM(Relation entity) {
		return new OSMRelation(entity.getId() //
				, modifiedVersion(entity) //
				, new OSHDBTimestamp(entity.getTimestamp()) //
				, entity.getChangeset() //
				, entity.getUserId() //
				, getKeyValue(entity.getTags()) //
				, convertToOSMMembers(entity.getMembers()));
	}

	private OSMMember[] convertToOSMMembers(RelationMember[] members) {
		OSMMember[] ret = new OSMMember[members.length];
		int i = 0;
		for (RelationMember member : members) {
			ret[i++] = new OSMMember(member.memId, OSMType.fromInt(member.type), getRole(member.role));
		}
		return ret;
	}

	private void storeRef(long cellId, LongSortedSet nodes, LongSortedSet ways) throws IOException {
		cellRefMap.add(cellId, nodes, ways);
	}
	
	@Override
	protected OSMType getType() {
		return OSMType.RELATION;
	}

	@Override
	protected long transform(long id, OSMType type, List<Entity> versions) throws IOException {
		List<OSMRelation> entities = new ArrayList<>(versions.size());
		LongSortedSet nodeIds = new LongAVLTreeSet();
		LongSortedSet wayIds = new LongAVLTreeSet();
		LongSortedSet relIds = new LongAVLTreeSet();
		for (Entity version : versions) {
			final Relation entity = (Relation) version;
			final OSMRelation osm = getOSM(entity);
			entities.add(osm);

			for (OSMMember member : osm.getMembers()) {
				final OSMType memType = member.getType();
				if (memType == OSMType.NODE) {
					nodeIds.add(member.getId());
				} else if (memType == OSMType.WAY) {
					wayIds.add(member.getId());
				}else if (memType == OSMType.RELATION){
					relIds.add(member.getId());
				}
			}
		}
		
		
		
		
		final LongSet cellIds;
		if (!nodeIds.isEmpty()) {
			cellIds = nodeToCell.get(nodeIds);
			cellIds.addAll(wayToCell.get(wayIds));
		} else {
			cellIds = wayToCell.get(wayIds);
		}
		
		final long cellId = findBestFittingCellId(cellIds);
		final long baseId = 0;
		final TransformOSHRelation osh = TransformOSHRelation.build(baData, baRecord, baAux, entities, nodeIds, wayIds, relIds, baseId, 0, 0, 0);
		final ByteBuffer record = ByteBuffer.wrap(baRecord.array(), 0, baRecord.length());
		
		if(relIds.isEmpty()){
			// simple relations with only node and way members
			store(cellId, record);
			storeRef(cellId, nodeIds,wayIds);
			idToCell(id, cellId);
		}else {
			nonSimpleRelSink.add(-1, record);
		}
		
		
		
		return id;
	}
	
	public static void transform(Args args, TagToIdMapper tagToId,RoleToIdMapper roleToId, CellDataSink cellDataSink, CellDataSink nonSimpleRelSink, CellRefSink cellRefSink,
			IdToCellSink idToCellSink, IdToCellSource nodeToCellSource, IdToCellSource wayToCellSource) throws IOException {
		Path pbf = args.pbf;

		int workerId = args.worker;
		int workerTotal = args.totalWorkers;

		final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf);
		final long start = pbfMeta.relationStart;
		final long end = pbfMeta.relationEnd;
		final long hardEnd = pbfMeta.relationEnd;

		long chunkSize = (long) Math.ceil((double) (end - start) / workerTotal);
		long chunkStart = start;
		long chunkEnd = chunkStart;

		if (workerTotal > 1) {
			chunkSize = (long) Math.ceil((double) (end - start) / workerTotal);
		} else {
			chunkSize = end - start;
		}

		for (int i = 0; i <= workerId; i++) {
			chunkStart = chunkEnd;
			chunkEnd = Math.min(chunkStart + chunkSize, end);
		}

		final Transform transform = Transform.of(pbf, chunkStart, chunkEnd, end, workerId);
		final TransformRelation tw = new TransformRelation(tagToId, roleToId, cellDataSink, nonSimpleRelSink, idToCellSink, nodeToCellSource, wayToCellSource, cellRefSink);
		final Stopwatch stopwatch = Stopwatch.createStarted();
		transform.transform(tw, () -> {
			System.out.println("complete!");
		});
		System.out.println(stopwatch);
	}
}
