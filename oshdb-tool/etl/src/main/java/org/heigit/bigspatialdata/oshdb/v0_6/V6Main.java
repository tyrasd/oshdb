package org.heigit.bigspatialdata.oshdb.v0_6;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;
import java.util.function.ToIntFunction;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.FileExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.load2.Load.Args;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.TransformerTagRoles;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagId;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.builder.OSHNodeBuilder;
import org.heigit.bigspatialdata.oshdb.v0_6.impl.osh.reader.OSHNodeReader;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerReader;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerWriter;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Node;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobReader;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobToOSHIterator;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.PbfBlob;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;
import org.heigit.bigspatialdata.oshpbf.parser.util.MyLambdaSubscriber;
import org.reactivestreams.Publisher;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.io.CountingOutputStream;

import crosby.binary.Osmformat;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;
import io.reactivex.schedulers.Schedulers;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class V6Main {
	
	public static class Args {
		@Parameter(names = { "-workDir", "--workingDir" }, description = "path to store the result files.", validateWith = DirExistValidator.class, required = true, order = 10)
		public Path workDir;
		
		@Parameter(names = {"--pbf" }, description = "path to pbf-File to import", validateWith = FileExistValidator.class, required = true, order = 0)
		public Path pbf;

	}

	public static void main(String[] args) throws IOException {
		
		final Args config = new Args();
		final JCommander jcom = JCommander.newBuilder().addObject(config).build();

		try {
			jcom.parse(args);
		} catch (ParameterException e) {
			System.out.println("");
			System.out.println(e.getLocalizedMessage());
			System.out.println("");
			jcom.usage();
			return;
		}
		
		
		//final Path dataDir = Paths.get("/home/rtroilo/data");
		final Path workDir = config.workDir; //dataDir.resolve("work/planet");

		final Path pbf =  config.pbf; //dataDir.resolve("planet-updated.osh.pbf"); // dataDir.resolve("heidelberg.osh.pbf");

		final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf);

		final int workerTotal = 1;
		final int workerId = 0;

		final long start = pbfMeta.nodeStart;
		final long end = pbfMeta.nodeEnd;

		final long chunkSize = (long) Math.ceil((double) (end - start) / workerTotal);
		long chunkStart = start;
		long chunkEnd = chunkStart;
		for (int i = 0; i <= workerId; i++) {
			chunkStart = chunkEnd;
			chunkEnd = Math.min(chunkStart + chunkSize, end);
		}

		final String prefix = "x_node";

		try (OutputStream indexOut = new BufferedOutputStream(
				new FileOutputStream(workDir.resolve(prefix + ".index").toFile()));
				OutputStream dataOut = new BufferedOutputStream(
						new FileOutputStream(workDir.resolve(prefix + ".data").toFile()))) {

			final TagToIdMapper tagToId = TransformerTagRoles.getTagToIdMapper(workDir);

			V6Main transformer = new V6Main(tagToId, indexOut, dataOut, workDir.resolve(prefix + ".grid").toString());

			Flowable<PbfBlob> blobFlow = read(pbf, chunkStart, chunkEnd, end, workerId);
			Flowable<BlobToOSHIterator> blockFlow = decompress(blobFlow);
			Flowable<Block> oshFlow = extract(blockFlow.limit(100));

			subscribe(oshFlow, transformer::transform, (e) -> e.printStackTrace(), transformer::complete, 1);
		}
	}

	private final TagToIdMapper tagToId;

	private long lastBlockId = -1;
	private Osh lastOsh = null;

	private final DataOutputStream outIndex;
	private final CountingOutputStream outData;
	private final String outGridPath;

	public V6Main(TagToIdMapper tagToId, OutputStream index, OutputStream outData, String outGridPath) {
		this.tagToId = tagToId;
		this.outIndex = new DataOutputStream(index);
		this.outData = new CountingOutputStream(outData);
		this.outGridPath = outGridPath;
	}

	public void transform(Block block) {
		System.out.printf("%12d:%6d - %s(%d) %10d->%10d%n", block.pos, block.id, block.type, block.oshs.size(),
				block.oshs.get(0).getId(), block.oshs.get(block.oshs.size() - 1).getId());

		for (Osh osh : block.oshs) {
			if (lastOsh != null && lastOsh.getType() != osh.getType()) {
				System.out.println("switch type from " + lastOsh.getType() + " to " + osh.getType());
				transform(lastOsh);
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
				System.out.println("merge id " + lastOsh.getId() + " from block " + lastBlockId + " with " + block.id);
				lastOsh.getVersions().addAll(osh.getVersions());
				continue;
			}

			transform(lastOsh);
			lastOsh = osh;
		}
		lastBlockId = block.id;
	}

	private void transform(Osh osh) {
		final long id = osh.getId();
		final List<Entity> entities = osh.getVersions();
		entities.sort((a, b) -> {
			int c = Integer.compare(a.getVersion(), b.getVersion());
			if (c == 0) {
				c = Long.compare(a.getTimestamp(), b.getTimestamp());
			}
			return c * OSHDB.sortOrder.dir;
		});

		switch (osh.getType()) {
		case NODE:
			transformNodes(id, entities);
			break;
		case WAY:
			break;
		case RELATION:
			break;
		default:
			System.err.println("unknown type " + osh);
		}
	}

	private IntSortedSet uidSet = new IntAVLTreeSet();
	private SortedSet<OSHDBTag> tagSet = new TreeSet<>();
	private final Object2IntMap<OSHDBTag> tagToIdx = new Object2IntOpenHashMap<>();
	private final Long2IntMap uidToIdx = new Long2IntOpenHashMap();

	private final FastByteArrayOutputStream out = new FastByteArrayOutputStream();
	private final FastByteArrayOutputStream auxOut = new FastByteArrayOutputStream();
	private final SerializationUtils serUtil = new SerializationUtils();
	private final DeltaIntegerWriter intWriter = new DeltaIntegerWriter("v6main", false);
	private final OSHNodeBuilder builder = new OSHNodeBuilder();

	private final DeltaIntegerReader intReader = new DeltaIntegerReader("v6main", false);
	private final OSHNodeReader reader = new OSHNodeReader();

	private void transformNodes(long id, List<Entity> entities) {
		final List<OSMNode> nodes = new ArrayList<>(entities.size());

		uidSet.clear();
		tagSet.clear();
		tagToIdx.clear();
		uidToIdx.clear();

		int maxVersion = Integer.MIN_VALUE;

		long minTimestamp, maxTimestamp;
		long minLongitude, maxLongitude;
		long minLatitude, maxLatitude;

		boolean lastVisibility = false;

		minTimestamp = minLongitude = minLatitude = Long.MAX_VALUE;
		maxTimestamp = maxLongitude = maxLatitude = Long.MIN_VALUE;

		for (Entity entity : entities) {
			final Node node = (Node) entity;
			final OSMNode osm = getNode(node);

			final int version = osm.getVersion();
			if (maxVersion < version) {
				maxVersion = version;
				lastVisibility = osm.isVisible();
			}

			final long timestamp = osm.getTimestamp().getRawUnixTimestamp();
			if (minTimestamp > timestamp) {
				minTimestamp = timestamp;
			}
			if (maxTimestamp < timestamp) {
				maxTimestamp = timestamp;
			}

			osm.getTags().forEach(tagSet::add);
			uidSet.add(osm.getUserId());

			if (osm.isVisible()) {
				final long longitude = osm.getLon();
				final long latitude = osm.getLat();

				if (minLongitude > longitude && longitude >= OSHDB.VALID_MIN_LONGITUDE) {
					minLongitude = longitude;
				}
				if (minLatitude > latitude && latitude >= OSHDB.VALID_MIN_LATITUDE) {
					minLatitude = latitude;
				}

				if (maxLongitude < longitude && longitude <= OSHDB.VALID_MAX_LONGITUDE) {
					maxLongitude = longitude;
				}
				if (maxLatitude < latitude && latitude <= OSHDB.VALID_MAX_LATITUDE) {
					maxLatitude = latitude;
				}
			}
			nodes.add(osm);
		}

		try {
			out.reset();
			int header = 0;
			if (nodes.size() == 1) {
				header |= OSHDB.OSH_HEADER_SINGLE;
			}
			if (lastVisibility) {
				header |= OSHDB.OSH_HEADER_VISIBLE;
			}
			if (maxVersion == nodes.size()) {
				header |= OSHDB.OSH_HEADER_COMPLETE;
			}
			if (tagSet.size() > 0) {
				header |= OSHDB.OSH_HEADER_HAS_TAGS;
			}
			if (minLongitude == maxLongitude && minLatitude == maxLatitude) {
				header |= OSHDB.OSH_HEADER_IS_POINT;
			} else if (minLongitude == Long.MAX_VALUE || minLatitude == Long.MAX_VALUE) {
				header |= OSHDB.OSH_HEADER_INVALID;
			}

			out.write(header);

			if ((header & OSHDB.OSH_HEADER_INVALID) == 0) {
				serUtil.writeVslong(out, minLongitude);
				serUtil.writeVslong(out, minLatitude);

				if ((header & OSHDB.OSH_HEADER_IS_POINT) == 0) {
					final long dLongitude = maxLongitude - minLongitude;
					final long dLatitude = maxLatitude - minLatitude;
					serUtil.writeVulong(out, dLongitude);
					serUtil.writeVulong(out, dLatitude);
				}
			}

			serUtil.writeVulong(out, minTimestamp);
			if ((header & OSHDB.OSH_HEADER_VISIBLE) == 0) {
				final long dTimestamp = maxTimestamp - minTimestamp;
				serUtil.writeVulong(out, dTimestamp);
			}

			if ((header & OSHDB.OSH_HEADER_HAS_TAGS) != 0) {
				serUtil.writeVulong(out, tagSet.size());
				intWriter.write(out, writer -> {
					int idx = 0;
					for (OSHDBTag tag : tagSet) {
						writer.write(tag.getKey());
						tagToIdx.put(tag, idx++);
					}
				});
				intWriter.write(out, writer -> {
					for (OSHDBTag tag : tagSet) {
						writer.write(tag.getValue());
					}
				});
			}

			if ((header & OSHDB.OSH_HEADER_SINGLE) == 0) {
				serUtil.writeVulong(out, uidSet.size());
				intWriter.write(out, writer -> {
					int idx = 0;
					for (int uid : uidSet) {
						writer.write(uid);
						uidToIdx.put(uid, idx++);
					}
				});
				LongToIntFunction uidLookup = uidToIdx::get;
				ToIntFunction<OSHDBTag> tagLookup = tagToIdx::getInt;
				builder.build(out, nodes, minTimestamp, minLongitude, minLatitude, uidLookup, tagLookup);
			} else {
				builder.buildSingleEntity(out, header, nodes.get(0));
			}

			
			auxOut.reset();
			serUtil.writeVulong(auxOut, out.length());
			auxOut.write(out.array, 0, out.length);
						
			ByteBuffer bytes = ByteBuffer.wrap(auxOut.array,0,auxOut.length);

//			if (!checkNodes(bytes, id, nodes)) {
//				System.out.println("debug");
//			}
			writeOut(id, minLongitude, minLatitude, maxLongitude, maxLatitude, tagSet, header, bytes);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	long nextId = 0;
	final long MB = 1L * 1024L * 1024L;
	final byte[] kb = new byte[1024];
	long pageSize = MB;
	ZGrid zGrid = new ZGrid(15);

	static final int MAX_SORT_SIZE = 25_000_000;
	final ArrayList<SortKey> gridSort = new ArrayList<>(MAX_SORT_SIZE);
	int sequence = 0;

	private void writeOut(long id, long minLongitude, long minLatitude, long maxLongitude, long maxLatitude, SortedSet<OSHDBTag> tagSet, int header, ByteBuffer bytes) throws IOException {

		for (; nextId < id; nextId++) {
			outIndex.writeLong(-1);
		}

		if ((pageSize - bytes.limit()) < 0) {
			// padding
			while (pageSize > 0) {
				int len = (int) Math.min(kb.length, pageSize);
				outData.write(kb, 0, len);
				pageSize -= len;
			}
			pageSize = MB;
		}

		long pos = outData.getCount();
		outIndex.writeLong(pos);
		outData.write(bytes.array(), 0, bytes.limit());
		nextId++;
		
		if(!tagSet.isEmpty() && ((header & OSHDB.OSH_HEADER_INVALID) == 0)){
			final long zId = zGrid.getIdSingleZIdWithZoom(minLongitude, maxLongitude, minLatitude, maxLatitude);
			final long midLongitude = maxLongitude - minLongitude;
			final long midLatitude = maxLatitude - minLatitude;
			gridSort.add(new SortKey(zId,midLongitude,midLatitude,id,pos));
			if(gridSort.size() == MAX_SORT_SIZE){
				writeOutGridSort();
				gridSort.clear();
			}
		}
	}
	
	private void writeOutGridSort() throws FileNotFoundException, IOException{
		try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(String.format("%s_%04d", outGridPath,sequence++))))){
			Iterator<SortKey> gridItr = gridSort.parallelStream().sorted().iterator();
			SortKey key = gridItr.next();
			long lastCellId = key.zId;
			
			serUtil.writeVulong(out,key.zId);
			serUtil.writeVulong(out,key.id+1);
			serUtil.writeVulong(out,key.pos);
			
			while(gridItr.hasNext()){
				key = gridItr.next();
				if(key.zId != lastCellId){
					serUtil.writeVulong(out,0);
					serUtil.writeVulong(out,key.zId - lastCellId);
										
					lastCellId = key.zId;
				}
				serUtil.writeVulong(out,key.id+1); // avoid 0 key.id
				serUtil.writeVulong(out,key.pos);
			}					
		}
	}

	private static class SortKey implements Comparable<SortKey> {
		public final long zId;
		public final long midLongitude;
		public final long midLatitude;
		public final long id;
		public final long pos;

		private SortKey(long zId, long midLongitude, long midLatitude, long id, long pos) {
			this.zId = zId;
			this.midLongitude = midLongitude;
			this.midLatitude = midLatitude;
			this.id = id;
			this.pos = pos;
		}

		@Override
		public int compareTo(SortKey o) {
			int c = Long.compare(zId, o.zId);
			if (c == 0) {
				c = Long.compare(midLongitude, o.midLongitude);
				if (c == 0) {
					c = Long.compare(midLatitude, o.midLatitude);
					if (c == 0)
						c = Long.compare(id, o.id);
				}
			}
			return c;
		}

	}
	
	private boolean checkNodes(ByteBuffer bytes, long id, List<OSMNode> nodes) throws IOException {
		final FastByteArrayInputStream in = new FastByteArrayInputStream(bytes.array(), bytes.arrayOffset(),bytes.limit());
		final int lenBytes = (int) SerializationUtils.readVulong(in);
		final int header = in.read();

		final long minLongitude, minLatitude;
		final long maxLongitude, maxLatitude;

		if ((header & OSHDB.OSH_HEADER_INVALID) == 0) {
			minLongitude = SerializationUtils.readVslong(in);
			minLatitude = SerializationUtils.readVslong(in);
			if ((header & OSHDB.OSH_HEADER_IS_POINT) == 0) {
				maxLongitude = SerializationUtils.readVulong(in) + minLongitude;
				maxLatitude = SerializationUtils.readVulong(in) + minLatitude;
			} else {
				maxLongitude = minLongitude;
				maxLatitude = minLatitude;
			}
		} else {
			minLongitude = minLatitude = Long.MAX_VALUE;
			maxLongitude = maxLatitude = Long.MIN_VALUE;
		}

		final long minTimestamp = SerializationUtils.readVulong(in);
		final long maxTimestamp;
		if ((header & OSHDB.OSH_HEADER_VISIBLE) == 0) {
			maxTimestamp = SerializationUtils.readVulong(in);
		} else {
			maxTimestamp = Long.MAX_VALUE;
		}

		final List<OSHDBTag> tags;
		if ((header & OSHDB.OSH_HEADER_HAS_TAGS) != 0) {
			int size = (int) SerializationUtils.readVulong(in);
			tags = new ArrayList<>(size);

			intReader.read(in, r -> {
				for (int i = 0; i < size; i++) {
					tags.add(new OSHDBTag((int) r.next(), -1));
				}
			});

			intReader.read(in, r -> {
				for (int i = 0; i < size; i++) {
					tags.get(i).setValue((int) r.next());
				}
			});
		} else {
			tags = Collections.emptyList();
		}
		if ((header & OSHDB.OSH_HEADER_SINGLE) == 0) {
			int size = (int) SerializationUtils.readVulong(in);
			final IntArrayList uids = new IntArrayList(size);
			intReader.read(in, r -> {
				for (int i = 0; i < size; i++) {
					uids.add((int) r.next());
				}
			});
			LongToIntFunction uidLookup = u -> uids.getInt((int) u);
			LongFunction<OSHDBTag> tagLookup = t -> tags.get((int) t);
			reader.reset(in, 0, id, minTimestamp, null, minLongitude, minLatitude, uidLookup, tagLookup);
		} else {
			reader.reset(in, header, id, minTimestamp, tags, minLongitude, minLatitude, idx -> (int) idx, null);
		}

		Iterator<OSMNode> expectedItr = nodes.iterator();
		boolean ok = true;
		while (reader.hasNext() && expectedItr.hasNext()) {
			OSMNode expected = expectedItr.next();
			OSMNode actual = reader.read();

			if (!actual.equalsTo(expected)) {
				System.out.println("E: " + expected);
				System.out.println("A: " + actual);
				ok = false;
			}
		}

		if (reader.hasNext() || expectedItr.hasNext()) {
			ok = false;
			while (reader.hasNext()) {
				System.out.println(" : " + reader.read());
			}

			while (expectedItr.hasNext()) {
				System.out.println(" : " + expectedItr.next());
			}
		}
		return ok;
	}

	private OSMNode getNode(Node entity) {
		return new OSMNode(entity.getId(), //
				modifiedVersion(entity), //
				new OSHDBTimestamp(entity.getTimestamp()), //
				entity.getChangeset(), //
				entity.getUserId(), //
				getKeyValue(entity.getTags()), //
				entity.getLongitude(), entity.getLatitude());
	}

	public int modifiedVersion(Entity entity) {
		return entity.getVersion() * (entity.isVisible() ? 1 : -1);
	}

	protected int[] getKeyValue(TagText[] tags) {
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

	public void complete() {
		transform(lastOsh);
		try {
			writeOutGridSort();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("complete");
	}

	private static Flowable<PbfBlob> read(Path pbf, long start, long softEnd, long hardEnd, int workerId)
			throws IOException {
		final BlobReader blobReader = BlobReader.newInstance(pbf, start, softEnd, hardEnd, workerId != 0);
		Flowable<PbfBlob> blobFlow = Flowable.generate((emitter) -> {
			if (blobReader.hasNext()) {
				PbfBlob blob = blobReader.next();
				emitter.onNext(blob);
				return;
			} else {
				if (blobReader.wasException()) {
					emitter.onError(blobReader.getException());
				} else {
					emitter.onComplete();
				}
			}
		});
		blobFlow = blobFlow.subscribeOn(Schedulers.io());
		blobFlow = blobFlow.filter(PbfBlob::isData);
		return blobFlow;
	}

	private static Flowable<BlobToOSHIterator> decompress(Flowable<PbfBlob> blobFlow) {
		Flowable<BlobToOSHIterator> blockFlow = blobFlow.observeOn(Schedulers.computation(), false, 1).map(blob -> {
			final Osmformat.PrimitiveBlock block = blob.getPrimitivBlock();
			return new BlobToOSHIterator(blob, block);
		});
		return blockFlow;
	}

	public static class Block {
		public final long pos;
		public final long id;

		public final OSMType type;
		public final List<Osh> oshs;

		public Block(long pos, long id, OSMType type, List<Osh> oshs) {
			this.pos = pos;
			this.id = id;
			this.type = type;
			this.oshs = oshs;
		}
	}

	private static Flowable<Block> extract(Flowable<BlobToOSHIterator> blockFlow) {
		Flowable<Block> oshFlow = blockFlow.observeOn(Schedulers.computation(), false, 1).map(block -> {
			block.getBlob().getBlobPos();
			List<Osh> oshs = new ArrayList<>(10_000);
			while (block.hasNext()) {
				oshs.add(block.next());
			}
			return new Block(block.getBlob().getBlobPos(), block.getBlob().getBlobId(), oshs.get(0).getType(), oshs);
		});
		return oshFlow;
	}

	public static <T> void subscribe(Publisher<? extends T> o, final Consumer<? super T> onNext,
			final Consumer<? super Throwable> onError, final Action onComplete, long requestValue) {
		ObjectHelper.requireNonNull(onNext, "onNext is null");
		ObjectHelper.requireNonNull(onError, "onError is null");
		ObjectHelper.requireNonNull(onComplete, "onComplete is null");
		FlowableBlockingSubscribe.subscribe(o, new MyLambdaSubscriber<T>(onNext, onError, onComplete, requestValue));
	}

}
