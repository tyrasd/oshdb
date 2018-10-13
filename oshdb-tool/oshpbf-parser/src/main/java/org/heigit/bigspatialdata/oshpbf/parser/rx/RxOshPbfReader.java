package org.heigit.bigspatialdata.oshpbf.parser.rx;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobReader;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobToOSHIterator;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.PbfBlob;
import org.heigit.bigspatialdata.oshpbf.parser.util.ByteBufferBackedInputStream;
import org.heigit.bigspatialdata.oshpbf.parser.util.MyLambdaSubscriber;
import org.reactivestreams.Publisher;

import com.google.protobuf.InvalidProtocolBufferException;

import crosby.binary.Fileformat;
import crosby.binary.Osmformat;
import crosby.binary.Osmformat.HeaderBlock;
import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.operators.flowable.FlowableBlockingSubscribe;
import io.reactivex.schedulers.Schedulers;;

public class RxOshPbfReader {

	public static Flowable<PbfBlob> readBlob(Path pbfPath, long start, long softLimit, long hardLimit, boolean skipFirst) throws IOException {
	final BlobReader blobReader = BlobReader.newInstance(pbfPath, start, softLimit, hardLimit, skipFirst);
    Flowable<PbfBlob> flow = Flowable.generate((emitter) -> {
		if (blobReader.hasNext()) {
			PbfBlob blob = blobReader.next();
			//queue.put(blob.getId());
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
    
    
    return flow.subscribeOn(Schedulers.io());
  }

  public static Flowable<Osh> readOsh(Path pbfPath, long start, long softLimit, long hardLimit, boolean skipFirst) throws IOException {
    return readOsh(pbfPath, start, softLimit, hardLimit, skipFirst, header -> {});
  }
  public static Flowable<Osh> readOsh(Path pbfPath, long start, long softLimit, long hardLimit, boolean skipFirst, Consumer<HeaderBlock> header) throws IOException {
    final int cpus = Runtime.getRuntime().availableProcessors();
    final int maxConcurrency = cpus - 1;
    final int prefetch = 4;

    Flowable<PbfBlob> blobFlow = readBlob(pbfPath, start, softLimit, hardLimit, skipFirst);
    blobFlow = blobFlow.doOnNext(blob -> {
        if(blob.isHeader())
          header.accept(blob.getHeaderBlock());
      });
     blobFlow = blobFlow.filter(PbfBlob::isData);
     
     Flowable<Osh> oshFlow = blobFlow.observeOn(Schedulers.computation(),false,1).concatMapEager(
             blob -> Flowable.just(blob).subscribeOn(Schedulers.computation()).map(b -> blobToOSHItr(b)),
             maxConcurrency, prefetch)
         .flatMap(Flowable::fromIterable);
   
    oshFlow = new OshMerger(oshFlow);
    return oshFlow;
  }

  public static Iterable<Osh> blobToOSHItr(PbfBlob blob) throws InvalidProtocolBufferException {
    return new Iterable<Osh>() {
      final Osmformat.PrimitiveBlock block = blob.getPrimitivBlock();

      @Override
      public Iterator<Osh> iterator() {
        return new BlobToOSHIterator(blob, block);
      }
    };
  }
  
  public static <T> void subscribe(Publisher<? extends T> o, final Consumer<? super T> onNext,
			final Consumer<? super Throwable> onError, final Action onComplete) {
		ObjectHelper.requireNonNull(onNext, "onNext is null");
		ObjectHelper.requireNonNull(onError, "onError is null");
		ObjectHelper.requireNonNull(onComplete, "onComplete is null");
		FlowableBlockingSubscribe.subscribe(o, new MyLambdaSubscriber<T>(onNext, onError, onComplete, 1L));
	}
  
}
