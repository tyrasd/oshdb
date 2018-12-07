package org.heigit.bigspatialdata.oshdb.v0_6.extract;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.orc.impl.SerializationUtils;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.FileExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.v0_6.transform.rx.Block;
import org.heigit.bigspatialdata.oshdb.v0_6.util.backref.BackRefSink;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.DeltaIntegerWriter;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Relation;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.RelationMember;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Way;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import crosby.binary.Osmformat;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;
import io.reactivex.schedulers.Schedulers;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class BackRefMain {

	private static class ExtractRefs {

		private final LongSortedSet refs = new LongRBTreeSet();
		private final BackRefSink backRefSink;

		public ExtractRefs(BackRefSink backRefSink) {
			this.backRefSink = backRefSink;
		}

		private void extract(Osh osh) throws FileNotFoundException, IOException {
			if (osh.getType() != OSMType.RELATION) {
				System.out.println("wrong type!!!!");
				System.exit(2);
				return;
			}

			long backRef = osh.getId();
			refs.clear();
			for (Entity entity : osh.getVersions()) {
				Relation o = (Relation) entity;

				for(RelationMember m : o.getMembers()){
					if(m.type == OSMType.RELATION.intValue()){
						refs.add(m.memId);
					}
				}
//				for (long ref : o.refs) {
//					refs.add(ref);
//				}
			}
			for (long ref : refs) {
				backRefSink.add(ref, backRef);
			}
		}

		private long lastBlockId = -1;
		private Osh lastOsh = null;

		public void extract(Block block) throws FileNotFoundException, IOException {
			for (Osh osh : block.oshs) {
				if (lastOsh != null && lastOsh.getType() != osh.getType()) {
					System.out.println("switch type from " + lastOsh.getType() + " to " + osh.getType());
					extract(lastOsh);
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
			//		System.out.println("merge id " + lastOsh.getId() + " from block " + lastBlockId + " with " + block.id);
					lastOsh.getVersions().addAll(osh.getVersions());
					continue;
				}

				extract(lastOsh);
				lastOsh = osh;
			}
			lastBlockId = block.id;

		}

		public void complete() throws FileNotFoundException, IOException {
			extract(lastOsh);
		}

	}

	public static class Args {
		@Parameter(names = { "-workDir",
				"--workingDir" }, description = "path to store the result files.", validateWith = DirExistValidator.class, required = true, order = 10)
		public Path workDir;

		@Parameter(names = {
				"--pbf" }, description = "path to pbf-File to import", validateWith = FileExistValidator.class, required = true, order = 0)
		public Path pbf;
	}

	public static void main(String[] args) throws IOException {
		Args config = new Args();
		JCommander jcom = JCommander.newBuilder().addObject(config).build();

		try {
			jcom.parse(args);
		} catch (ParameterException e) {
			System.out.println("");
			System.out.println(e.getLocalizedMessage());
			System.out.println("");
			jcom.usage();
			return;
		}

		final Path workDir = config.workDir;
		final Path pbf = config.pbf;

		final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf);

		final long start = pbfMeta.relationStart;
		final long end = pbfMeta.relationEnd;

		Flowable<PbfBlob> blobFlow = read(pbf, start, end, end, 0);
		Flowable<BlobToOSHIterator> blockFlow = decompress(blobFlow);
		Flowable<Block> oshFlow = extract(blockFlow);

		try (BackRefSink backRefSink = new BackRefSink(workDir.resolve("backRefs_relation_relation").toString(), 25_000_000)) {
			ExtractRefs extract = new ExtractRefs(backRefSink);
			Stopwatch stopwatch = Stopwatch.createStarted();
			subscribe(oshFlow, extract::extract, (e) -> e.printStackTrace(), extract::complete, 1);
			System.out.println(stopwatch);
		}

	}

	protected static Flowable<PbfBlob> read(Path pbf, long start, long softEnd, long hardEnd, int workerId)
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

	protected static Flowable<BlobToOSHIterator> decompress(Flowable<PbfBlob> blobFlow) {
		Flowable<BlobToOSHIterator> blockFlow = blobFlow.observeOn(Schedulers.computation(), false, 1).map(blob -> {
			final Osmformat.PrimitiveBlock block = blob.getPrimitivBlock();
			return new BlobToOSHIterator(blob, block);
		});
		return blockFlow;
	}

	protected static Flowable<Block> extract(Flowable<BlobToOSHIterator> blockFlow) {
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
