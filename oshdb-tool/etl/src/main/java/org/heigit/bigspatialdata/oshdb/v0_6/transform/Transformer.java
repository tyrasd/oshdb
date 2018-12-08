package org.heigit.bigspatialdata.oshdb.v0_6.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.LongToIntFunction;
import java.util.function.ToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;
import org.heigit.bigspatialdata.oshdb.v0_6.transform.rx.Block;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerWriter;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.OSHGridSort.SortKey;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public abstract class Transformer {
	protected static final long[] EMPTY_BACKREFS = new long[0];
	private final TagToIdMapper tagToId;
	private long lastBlockId = -1;
	private Osh lastOsh = null;

	protected final ZGrid zGrid = new ZGrid(OSHDB.MAX_ZOOM);

	protected IntSortedSet uidSet = new IntAVLTreeSet();
	protected SortedSet<OSHDBTag> tagSet = new TreeSet<>();
	protected final Object2IntMap<OSHDBTag> tagToIdx = new Object2IntOpenHashMap<>();
	protected final Long2IntMap uidToIdx = new Long2IntOpenHashMap();
	
	protected final LongToIntFunction uidLookup = uidToIdx::get;
	protected final ToIntFunction<OSHDBTag> tagLookup = tagToIdx::getInt;
	
	protected int maxVersion = Integer.MIN_VALUE;
	protected long minTimestamp, maxTimestamp;
	protected boolean lastVisibility = false;
	protected boolean multiVersion = false;
	protected boolean complete =  false;

	protected final SerializationUtils serUtil = new SerializationUtils();
	protected final DeltaIntegerWriter intWriter = new DeltaIntegerWriter("", false);

	public Transformer(TagToIdMapper tagToId) {
		this.tagToId = tagToId;
	}

	protected void transform(Block block) {
		for (Osh osh : block.oshs) {
			if (lastOsh != null && lastOsh.getType() != osh.getType()) {
				System.out.println("switch type from " + lastOsh.getType() + " to " + osh.getType());
				transform(lastOsh,block);
				lastOsh = null;
			}

			if (lastOsh == null) {
				lastOsh = osh;
				System.out.printf("start %s with id:%d%n", lastOsh.getType(), lastOsh.getId());
				continue;
			}

			if (osh.getId() < lastOsh.getId()) {
				System.err.printf("id's not in order! %d < %d %n", osh.getId(), lastOsh.getId());
				continue;
			}

			if (lastOsh.getId() == osh.getId()) {
//				System.out.println("merge id " + lastOsh.getId() + " from block " + lastBlockId + " with " + block.id);
				lastOsh.getVersions().addAll(osh.getVersions());
				continue;
			}

			transform(lastOsh, block);
			lastOsh = osh;
		}
		lastBlockId = block.id;
	}

	protected void transform(Osh osh,Block block) {
		final long id = osh.getId();
		final List<Entity> entities = osh.getVersions();
		entities.sort((a, b) -> {
			int c = Integer.compare(a.getVersion(), b.getVersion());
			if (c == 0) {
				c = Long.compare(a.getTimestamp(), b.getTimestamp());
			}
			return c * OSHDB.sortOrder.dir;
		});

		uidSet.clear();
		tagSet.clear();
		tagToIdx.clear();
		uidToIdx.clear();

		maxVersion = Integer.MIN_VALUE;
		lastVisibility = false;

		minTimestamp = Long.MAX_VALUE;
		maxTimestamp = Long.MIN_VALUE;

		transformEntities(id, osh.getType(), entities,block);
	}

	protected abstract void transformEntities(long id, OSMType type, List<Entity> entities,Block block);

	protected void complete() {
		transform(lastOsh,null);
	}

	protected List<OSHDBTag> getKeyValue(TagText[] tags) {
		if (tags.length == 0)
			return Collections.emptyList();

		final List<OSHDBTag> ids = new ArrayList<>(tags.length);

		for (TagText tag : tags) {
			final int key = tagToId.getKey(tag.key);
			final int value = tagToId.getValue(key, tag.value);
			ids.add(new OSHDBTag(key, value));
		}
		Collections.sort(ids);
		return ids;
	}
	
}
