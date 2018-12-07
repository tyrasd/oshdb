package org.heigit.bigspatialdata.oshdb.v0_6.transform.rx;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.internal.fuseable.QueueSubscription;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.plugins.RxJavaPlugins;

public class BlockMerger extends Flowable<BlockOsh> {

	private final Flowable<BlockOsh> source;

	public static Flowable<BlockOsh> merge(Flowable<BlockOsh> source) {
		return new BlockMerger(source);
	}

	private BlockMerger(Flowable<BlockOsh> source) {
		this.source = source;
	}

	@Override
	protected void subscribeActual(Subscriber<? super BlockOsh> actual) {
		source.subscribe(new MergerSubscriber(actual));

	}

	private static final class MergerSubscriber implements FlowableSubscriber<BlockOsh>, Subscription {
		/** The downstream subscriber. */
		private final Subscriber<? super BlockOsh> actual;

		/** The upstream subscription. */
		private Subscription s;

		/** Flag indicating no further onXXX event should be accepted. */
		private boolean done;

		private long lastBlockId = -1;
		private BlockOsh lastOsh = null;

		private MergerSubscriber(Subscriber<? super BlockOsh> actual) {
			this.actual = actual;
		}

		@Override
		public void onNext(BlockOsh block) {
			
			if (lastOsh != null && lastOsh.type != block.type) {
				System.out.println("switch type from " + lastOsh.type + " to " + block.type);
				actual.onNext(lastOsh);				
				lastOsh = null;
			}

			if (lastOsh == null) {
				lastOsh = block;
				System.out.printf("start %s with id:%d%n", lastOsh.type, lastOsh.osh.getId());
				s.request(1);
				return;
			}

			if (block.osh.getId() < lastOsh.osh.getId()) {
				System.err.printf("id's not in order! %d < %d %n", block.osh.getId(), lastOsh.osh.getId());
				s.request(1);
				return;
			}

			if (lastOsh.osh.getId() == block.osh.getId()) {
				System.out.println(
						"merge id " + lastOsh.osh.getId() + " from block " + lastBlockId + " with " + block.id);
				lastOsh.osh.getVersions().addAll(block.osh.getVersions());
				s.request(1);
				return;
			}
			actual.onNext(lastOsh);
			lastOsh = block;
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			
			actual.onNext(lastOsh);
			actual.onComplete();

		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				RxJavaPlugins.onError(t);
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
	    public void cancel() {
		  lastOsh = null;
	      s.cancel();
	    }

		
		@Override
		public void request(long n) {
			s.request(1);
		}

		@Override
	    public final void onSubscribe(Subscription s) {
	      if (SubscriptionHelper.validate(this.s, s)) {
	        this.s = s;
	        if (s instanceof QueueSubscription) {
	          System.out.println("s instanceof QueueSubscription");
	        }
	        actual.onSubscribe(this);
	      }
	    }
	}
}
