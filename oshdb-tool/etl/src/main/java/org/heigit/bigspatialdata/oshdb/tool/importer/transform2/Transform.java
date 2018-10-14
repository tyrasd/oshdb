package org.heigit.bigspatialdata.oshdb.tool.importer.transform2;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.DirExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.cli.validator.FileExistValidator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobReader;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobToOSHIterator;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.PbfBlob;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;
import org.heigit.bigspatialdata.oshpbf.parser.rx.OshMerger;
import org.heigit.bigspatialdata.oshpbf.parser.util.MyLambdaSubscriber;
import org.reactivestreams.Publisher;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.validators.PositiveInteger;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import crosby.binary.Osmformat;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;
import io.reactivex.schedulers.Schedulers;

public class Transform {

	public static class Args {
		@Parameter(names = { "-workDir",
				"--workingDir" }, description = "path to store the result files.", validateWith = DirExistValidator.class, required = false, order = 10)
		public Path workDir = Paths.get(".");

		@Parameter(names = {
				"--worker" }, description = "number of this worker (beginning with 0).", validateWith = PositiveInteger.class, required = false, order = 20)
		public int worker = 0;

		@Parameter(names = {
				"--totalWorker" }, description = "total number of workers.", validateWith = PositiveInteger.class, required = false, order = 21)
		public int totalWorkers = 1;

		@Parameter(names = {
				"--pbf" }, description = "path to pbf-File to import", validateWith = FileExistValidator.class, required = true, order = 0)
		public Path pbf;
	}

	private Path pbf;
	private int workerId;

	private long start;
	private long softEnd;
	private long hardEnd;

	private Transform(Path pbf, long start, long softEnd, long hardEnd, int workerId) {
		this.pbf = pbf;
		this.start = start;
		this.softEnd = softEnd;
		this.hardEnd = hardEnd;
		this.workerId = workerId;
	}

	public static Transform of(Path pbf, long start, long softEnd, long hardEnd, int workerId) {
		return new Transform(pbf, start, softEnd, hardEnd, workerId);
	}

	public static Args parse(String[] args) {
		Args config = new Args();
		JCommander jcom = JCommander.newBuilder().addObject(config).build();

		try {
			jcom.parse(args);
		} catch (ParameterException e) {
			System.out.println("");
			System.out.println(e.getLocalizedMessage());
			System.out.println("");
			jcom.usage();
			return null;
		}
		return config;
	}

	public void transform(Transformer transform, Action onComplete) throws IOException {
		Flowable<PbfBlob> blobFlow = read(pbf, start, softEnd, hardEnd, workerId);
		Flowable<BlobToOSHIterator> blockFlow = decompress(blobFlow);
		Flowable<List<Osh>> oshFlow = extract(blockFlow);

		// Flowable<Long> resultFlow =
		// oshFlow.observeOn(Schedulers.computation(), false,
		// 1).map(transform::transform);

		// subscribe(resultFlow.limit(10_000), (id)-> {},
		// Throwable::printStackTrace, onComplete, 1L);

		subscribe(oshFlow, (osh) -> {
			transform.transform(osh);
		}, Throwable::printStackTrace, () -> {
			transform.complete();
			onComplete.run();
		}, 1L);

	}

	private Flowable<List<Osh>> extract(Flowable<BlobToOSHIterator> blockFlow) {
		Flowable<List<Osh>> oshFlow = blockFlow.observeOn(Schedulers.computation(), false, 1).map(block -> {
			List<Osh> oshs = Lists.newArrayList();
			Stopwatch stopwatch = Stopwatch.createStarted();
			while (block.hasNext()) {
				oshs.add(block.next());
			}
			// System.out.println(block.getBlob().blobId + "=generate
			// versions:"+ oshs.size() + "(" + stopwatch + "): " +
			// Thread.currentThread().getName());
			return oshs;
		});
		return oshFlow;
	}

	private Flowable<BlobToOSHIterator> decompress(Flowable<PbfBlob> blobFlow) {
		Flowable<BlobToOSHIterator> blockFlow = blobFlow.observeOn(Schedulers.computation(), false, 1).map(blob -> {
			// Stopwatch stopwatch = Stopwatch.createStarted();
			final Osmformat.PrimitiveBlock block = blob.getPrimitivBlock();
			// System.out.println(blob.getBlobId() + "=decompress(" + stopwatch
			// + "): " + Thread.currentThread().getName());
			return new BlobToOSHIterator(blob, block);
		});
		return blockFlow;
	}

	private Flowable<PbfBlob> read(Path pbf, long start, long softEnd, long hardEnd, int workerId) throws IOException {
		final BlobReader blobReader = BlobReader.newInstance(pbf, start, softEnd, hardEnd, workerId != 0);
		Flowable<PbfBlob> blobFlow = Flowable.generate((emitter) -> {
			if (blobReader.hasNext()) {
				PbfBlob blob = blobReader.next();
				emitter.onNext(blob);
				// System.out.println(blob.getBlobId() + "=generater.onNext: " +
				// Thread.currentThread().getName());
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

	public static <T> void subscribe(Publisher<? extends T> o, final Consumer<? super T> onNext,
			final Consumer<? super Throwable> onError, final Action onComplete, long requestValue) {
		ObjectHelper.requireNonNull(onNext, "onNext is null");
		ObjectHelper.requireNonNull(onError, "onError is null");
		ObjectHelper.requireNonNull(onComplete, "onComplete is null");
		FlowableBlockingSubscribe.subscribe(o, new MyLambdaSubscriber<T>(onNext, onError, onComplete, requestValue));
	}
}
