package org.heigit.bigspatialdata.oshdb.tool.importer.transform2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.CellDataMap;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagId;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell.IdToCellSink;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;

public abstract class Transformer {

	private final ZGrid grid = new ZGrid(15);
	private final TagToIdMapper tagToId;
	private final RoleToIdMapper roleToId; 
	private final CellDataMap cellDataMap;
	private final IdToCellSink idToCellSink;

	
	public Transformer(TagToIdMapper tagToId, CellDataMap cellDataMap, IdToCellSink idToCellSink) {
		this(tagToId, null, cellDataMap, idToCellSink);
	}
	
	public Transformer(TagToIdMapper tagToId, RoleToIdMapper roleToId, CellDataMap cellDataMap, IdToCellSink idToCellSink) {
		this.tagToId = tagToId;
		this.roleToId = roleToId;
		this.cellDataMap = cellDataMap;
		this.idToCellSink = idToCellSink;
	}

	private long lastId = -1;
	
	private Osh lastOsh = null;

	public long transform(List<Osh> oshs) throws IOException {
		for (Osh osh : oshs) {
			if(lastOsh == null){
				lastOsh = osh;
				continue;
			}
			
							
			
			if(lastOsh.getId() == osh.getId()){
				lastOsh.getVersions().addAll(osh.getVersions());
				continue;
			}
			

			final long id = lastOsh.getId();
			final OSMType type = lastOsh.getType();
			final List<Entity> versions = lastOsh.getVersions();
			lastOsh = osh;			

			if (id < lastId) {
				System.err.printf("id's not in order! %d < %d %n", id, lastId);
				continue;
			}
			if (lastId == -1) {
				System.out.printf("start with first id:%d%n", id);
			}
			lastId = id;

			if (type != getType()) {
				System.err.printf("wrong type expect:%s but got %s%n", getType(), type);
				continue;
			}

			transform(id, type, versions);
		}
		return 0;
	}
	
	public void complete() throws IOException{
		transform(lastOsh.getId(), lastOsh.getType(), lastOsh.getVersions());
		System.out.printf("end with last id:%d%n",lastOsh.getId());
		lastOsh = null;
	}

	protected abstract long transform(long id, OSMType type, List<Entity> versions) throws IOException;

	protected abstract OSMType getType();

	protected void store(long cellId, ByteBuffer record) throws IOException {
		cellDataMap.add(cellId, record);
	}

	protected void idToCell(long id, long cellId) throws IOException {
		this.idToCellSink.put(id, cellId);
	}

	public int modifiedVersion(Entity entity) {
		return entity.getVersion() * (entity.isVisible() ? 1 : -1);
	}

	public int[] getKeyValue(TagText[] tags) {
		if (tags.length == 0)
			return new int[0];

		final List<TagId> ids = new ArrayList<>(tags.length);

		for (TagText tag : tags) {
			final int key = tagToId.getKey(tag.key);
			final int value = tagToId.getValue(key, tag.value);
			ids.add(TagId.of(key, value));
		}

		ids.sort((a, b) -> {
			final int c = Integer.compare(a.key, b.key);
			return (c != 0) ? c : Integer.compare(a.value, b.value);
		});
		final int[] ret = new int[tags.length * 2];
		int i = 0;
		for (TagId tag : ids) {
			ret[i++] = tag.key;
			ret[i++] = tag.value;
		}

		return ret;
	}
	
	public int getRole(String role) {
		return (roleToId != null) ? roleToId.getRole(role) : 0;
	}
	

	protected long getCell(long longitude, long latitude) {
		return grid.getIdSingleZIdWithZoom(longitude, latitude);
	}

	protected static long findBestFittingCellId(Set<Long> cellIds) {
		if (cellIds.isEmpty())
			return -1;

		if (cellIds.size() == 1)
			return cellIds.iterator().next();

		int minZoom = Integer.MAX_VALUE;
		for (Long cellId : cellIds) {
			minZoom = Math.min(minZoom, ZGrid.getZoom(cellId));
		}
		final int zoom = minZoom;
		// bring all to the same zoom level
		Set<Long> bestCellId = cellIds.stream().filter(id -> id >= 0).map(id -> ZGrid.getParent(id, zoom))
				.collect(Collectors.toSet());

		while (bestCellId.size() > 1) {
			cellIds = bestCellId;
			bestCellId = cellIds.stream().map(id -> ZGrid.getParent(id)).collect(Collectors.toSet());
		}
		final long cellId = bestCellId.iterator().next();
		return cellId;
	}

	protected OSHDBBoundingBox getCellBounce(long cellId) {
		return ZGrid.getBoundingBox(cellId);
	}
}
