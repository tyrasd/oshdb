package org.heigit.bigspatialdata.oshdb.v0_6.transform;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.v0_6.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.OSHNodeReader;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder.OSHNodeSerializer;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osm.OSMNodeImpl;
import org.heigit.bigspatialdata.oshdb.v0_6.util.backref.BackRefReader;
import org.heigit.bigspatialdata.oshdb.v0_6.util.backref.RefToBackRefs;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.BytesSink;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.OSHGridSort;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Node;

import com.google.common.collect.PeekingIterator;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class TransformerNode extends Transformer {
	private final FastByteArrayOutputStream out = new FastByteArrayOutputStream(4096);

	private final ZGrid zGrid = new ZGrid(OSHDB.MAX_ZOOM);
	
	private final BytesSink store;
	private final OSHGridSort sort;
	private final PeekingIterator<RefToBackRefs> backRefsWays;
	private final PeekingIterator<RefToBackRefs> backRefsRelations;
	private final ObjectArrayList<OSMNode> versions = new ObjectArrayList<>();

	private final OSHNodeSerializer oshSerializer = new OSHNodeSerializer();

	public TransformerNode(TagToIdMapper tagToId, BytesSink store, PeekingIterator<RefToBackRefs> backRefsWays, PeekingIterator<RefToBackRefs> backRefsRelations, OSHGridSort sort) {
		super(tagToId);
		this.store = store;
		this.sort = sort;
		this.backRefsWays = backRefsWays;
		this.backRefsRelations = backRefsRelations;
	}
	
	@Override
	protected void transformEntities(long id, OSMType type, List<Entity> entities) {
		oshSerializer.reset();
		versions.ensureCapacity(entities.size());
		for (Entity entity : entities) {
			final Node node = (Node) entity;
			final OSMNode osm = getOSM(node);
			oshSerializer.addNode(osm);
			versions.add(osm);
		}
		
		while(backRefsWays.hasNext() && backRefsWays.peek().ref < id){
			//skip
			backRefsWays.next();
		}
		final long[] ways;
		if(backRefsWays.hasNext() && backRefsWays.peek().ref == id){
			ways = backRefsWays.next().backRefs;
		}else{
			ways = EMPTY_BACKREFS;
		}
		
		while(backRefsRelations.hasNext() && backRefsRelations.peek().ref < id){
			//skip
			backRefsRelations.next();
		}
		final long[] relations;
		if(backRefsRelations.hasNext() && backRefsRelations.peek().ref == id){
			relations = backRefsRelations.next().backRefs;
		}else{
			relations = EMPTY_BACKREFS;
		}
		
		try {
			out.reset();
			oshSerializer.serialize(out, versions, ways, relations);
			
			long zid = zGrid.getIdSingleZIdWithZoom(oshSerializer.getMinLongitude(),oshSerializer.getMaxLongitude(), oshSerializer.getMinLatitude(), oshSerializer.getMaxLatitude());
			
			ByteBuffer bytes = ByteBuffer.wrap(out.array, 0, out.length);
			if (!check(versions, id, bytes)) {
				System.exit(2);
			}
			long pos = store.write(id, bytes);
			if(oshSerializer.hasTags()){
				sort.add(new GridSortKey(zid,id,pos));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		versions.clear();
	}

	private boolean check(ObjectArrayList<OSMNode> versions, long id, ByteBuffer bytes) throws IOException {
		OSHNodeReader reader = new OSHNodeReader();
		reader.read(id, bytes);

		boolean check = true;
		Iterator<OSMNode> expectedItr = versions.iterator();
		Iterator<OSMNode> actualItr = reader.iterator();
		while (expectedItr.hasNext() && actualItr.hasNext()) {
			OSMNode e = expectedItr.next();
			OSMNode a = actualItr.next();

//			if(true){
			if (!e.equals(a)) {
				if (!e.isVisible() && !a.isVisible())
					continue;
				System.out.println("e: " + e);
				System.out.println("a: " + a);
				check = false;
			}
		}

		while (expectedItr.hasNext()) {
			System.out.println("e: " + expectedItr.next());
			System.out.println("a: missing");
			check = false;
		}
		while (actualItr.hasNext()) {
			System.out.println("e: missing");
			System.out.println("a: " + actualItr.next());
			check = false;
		}

		return check;
	}

	private OSMNode getOSM(Node entity) {
		return new OSMNodeImpl(entity.getId(), entity.getVersion(), entity.isVisible(),
				new OSHDBTimestamp(entity.getTimestamp()), entity.getChangeset(), entity.getUserId(),
				getKeyValue(entity.getTags()), entity.getLongitude(), entity.getLatitude());
	}
}
