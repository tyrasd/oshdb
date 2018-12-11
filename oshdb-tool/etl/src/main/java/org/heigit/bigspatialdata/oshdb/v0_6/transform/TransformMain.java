 package org.heigit.bigspatialdata.oshdb.v0_6.transform;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.FileExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.transform.TransformerTagRoles;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagId;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.v0_6.transform.rx.Block;
import org.heigit.bigspatialdata.oshdb.v0_6.util.LongKeyValueSink;
import org.heigit.bigspatialdata.oshdb.v0_6.util.backref.BackRefReader;
import org.heigit.bigspatialdata.oshdb.v0_6.util.backref.RefToBackRefs;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.BytesSink;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.BytesSource;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.FullIndexByteStore;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.FullIndexStore;
import org.heigit.bigspatialdata.oshdb.v0_6.util.io.OSHGridSort;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobReader;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobToOSHIterator;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.PbfBlob;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;
import org.heigit.bigspatialdata.oshpbf.parser.util.MyLambdaSubscriber;
import org.reactivestreams.Publisher;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

import crosby.binary.Osmformat;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;
import io.reactivex.schedulers.Schedulers;

public class TransformMain {
	
	private static enum Step {
		Node, Way, Relation
	}

	public static class StepConverter implements IStringConverter<Step>, IParameterValidator {
		@Override
		public void validate(String name, String value) throws ParameterException {
			Step step = convert(value);
			if (step == null)
				throw new ParameterException(value + " for parameter " + name
						+ " is not a valid value. Allowed values are (n,node,w,way,r,relation)");
		}

		@Override
		public Step convert(String value) {
			final String step = value.trim().toLowerCase();
			switch (step) {
			case "n":
			case "node":
				return Step.Node;
			case "w":
			case "way":
				return Step.Way;
			case "r":
			case "relation":
				return Step.Relation;
			default:
				return null;
			}
		}
	}

	public static class Args {
		@Parameter(names = { "-workDir",
				"--workingDir" }, description = "path to store the result files.", validateWith = DirExistValidator.class, required = true, order = 10)
		public Path workDir;

		@Parameter(names = {
				"--pbf" }, description = "path to pbf-File to import", validateWith = FileExistValidator.class, required = true, order = 0)
		public Path pbf;

		@Parameter(names = { "-s",
				"--step" }, description = "step for transformation (node|way|relation)", validateWith = StepConverter.class, converter = StepConverter.class, required = true, order = 1)
		Step step;

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
		final Step step = config.step;
		final String prefix = step.toString().toLowerCase()+"s";

		final OsmPbfMeta pbfMeta = Extract.pbfMetaData(pbf);

		System.out.print("loading keytables ... ");
		Stopwatch stopwatch = Stopwatch.createStarted();
		final TagToIdMapper tagToId;
//		tagToId  =  TransformerTagRoles.getTagToIdMapper(workDir);
		
		tagToId = new TagToIdMapper() {
			Map<String, Integer> tags = new HashMap<>();
			
			@Override
			public int getValue(int key, String value) {
				return tags.computeIfAbsent(value, i -> tags.size());
			}
			
			@Override
			public TagId getTag(String key, String value) {
				int k = getKey(key);
				int v = getValue(k, value);
				return TagId.of(k, v);
			}
			
			@Override
			public int getKey(String key) {
				return tags.computeIfAbsent(key, i -> tags.size());
			}
			
			@Override
			public long estimatedSize() {
				// TODO Auto-generated method stub
				return 0;
			}
		};
	
		System.out.println(stopwatch);
		final String path = workDir.resolve(prefix).toString();
		try ( BytesSink store = FullIndexByteStore.getSink(path);
			  //LongKeyValueSink sort = FullIndexStore.sink(path + ".grid");
			  OSHGridSort sort = new OSHGridSort(250_000_000, workDir.resolve(prefix + ".grid").toString());) {
			
			final long start;
			final long end;
			final Transformer transformer;
			
			switch (step) {
			case Node: {
				start = pbfMeta.nodeStart;
				end = pbfMeta.nodeEnd;
				final PeekingIterator<RefToBackRefs> ways = Iterators.peekingIterator(BackRefReader.of(workDir.resolve("backRefs_node_way")));
				final PeekingIterator<RefToBackRefs> relations = Iterators.peekingIterator(BackRefReader.of(workDir.resolve("backRefs_node_relation")));
				transformer = new TransformerNode(tagToId, store, ways,relations,sort);
				break;
			}
			case Way: {
				start = pbfMeta.wayStart;
				end = pbfMeta.wayEnd;
				final PeekingIterator<RefToBackRefs> relations = Iterators.peekingIterator(BackRefReader.of(workDir.resolve("backRefs_way_relation")));
				BytesSource nodeSource = FullIndexByteStore.getSource(workDir.resolve("nodes").toString());
				transformer = new TransformerWay(tagToId, store, nodeSource, relations, sort);
				break;
			}
			default:
				System.out.println("step " + step + " not implemented yet!");
				return;
			}

			Flowable<PbfBlob> blobFlow = read(pbf, start, end, end, 0);
			Flowable<BlobToOSHIterator> blockFlow = decompress(blobFlow);
			Flowable<Block> oshFlow = extract(blockFlow);
			
			subscribe(oshFlow, transformer::transform, (e) -> e.printStackTrace(), transformer::complete, 1);
		}
	}
	
	private static OutputStream bufferedOutputStream(String path) throws FileNotFoundException{
		return new BufferedOutputStream(new FileOutputStream(path));
	}

	private static <T extends Comparable<T>> PeekingIterator<T> merge(Path workDir, String glob, Function<Path,Iterator<T>> get) throws IOException {
		List<Iterator<T>> readers = Lists.newArrayList();
		for (Path path : Files.newDirectoryStream(workDir, glob)) {
			readers.add(get.apply(path));
		}
		return Iterators.peekingIterator(Iterators.mergeSorted(readers,(a,b) -> a.compareTo(b)));
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
