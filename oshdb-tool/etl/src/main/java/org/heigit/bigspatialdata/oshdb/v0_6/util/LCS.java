package org.heigit.bigspatialdata.oshdb.v0_6.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.v0_6.OSHDB;

import com.google.common.collect.Lists;

public class LCS {

	public enum ActionType {
		TAKE(OSHDB.ACTION_TAKE), SKIP(OSHDB.ACTION_SKIP), UPDATE(OSHDB.ACTION_UPDATE), ADD(OSHDB.ACTION_ADD);
		public final int bits;

		private ActionType(int bits) {
			this.bits = bits;
		}
	}
	
	public static final EnumSet<LCS.ActionType> actionWithDiff = EnumSet.of(LCS.ActionType.ADD, LCS.ActionType.UPDATE);

	public class Action {
		public final ActionType type;
		public final int length;

		private Action(ActionType type, int length) {
			this.type = type;
			this.length = length;
		}

		@Override
		public String toString() {
			return String.format("%s:%d", type, length);
		}
	}

	public class LCSResult<U> {
		public final List<Action> actions;
		public final List<U> diffs;

		private LCSResult(List<Action> actions, List<U> diffs) {
			this.actions = actions;
			this.diffs = diffs;
		}
	}

	public <T extends Comparable<T>> LCSResult<T> sequence(List<T> a, List<T> b) {
		final int aSize = a.size();
		final int bSize = b.size();

		int commonPrefix = 0;
		while (aSize > commonPrefix && bSize > commonPrefix
				&& a.get(commonPrefix).compareTo(b.get(commonPrefix)) == 0) {
			commonPrefix++;
		}

		if (commonPrefix == aSize && bSize == commonPrefix) {
			return new LCSResult<T>(Collections.emptyList(), Collections.emptyList());
		}

		List<Action> actions = Lists.newArrayList();
		List<T> diffs = Lists.newArrayList();
		if (commonPrefix > 0) {
			actions.add(new Action(ActionType.TAKE, commonPrefix));
		}

		if (commonPrefix == bSize) {
			return new LCSResult<>(actions, Collections.emptyList());
		}

		int commonSuffix = 0;
		for (int aPos = aSize - 1, bPos = bSize - 1; aPos >= commonPrefix && bPos >= commonPrefix
				&& a.get(aPos).compareTo(b.get(bPos)) == 0; aPos--, bPos--) {
			commonSuffix++;
		}

		int aDiff = aSize - commonPrefix - commonSuffix;
		int bDiff = bSize - commonPrefix - commonSuffix;

		if (aDiff > 0 && bDiff == 0) {
			actions.add(new Action(ActionType.SKIP, aDiff));
		} else if (aDiff == 0 && bDiff > 0) {
			actions.add(new Action(ActionType.ADD, bDiff));
			for (int bPos = commonPrefix, diff = bDiff; diff > 0; bPos++, diff--) {
				diffs.add(b.get(bPos));
			}
		} else {
			generateLCS(commonPrefix, commonSuffix, a, aDiff, b, bDiff, actions, diffs);
		}

		if (commonSuffix > 0) {
			actions.add(new Action(ActionType.TAKE, commonSuffix));
		} else {
			if (actions.get(actions.size() - 1).type == ActionType.SKIP) {
				actions.remove(actions.size() - 1);
			}
		}

		return new LCSResult<>(actions, diffs);
	}

	private int[] matrix = new int[0];

	private <T extends Comparable<T>> void generateLCS(int commonPrefix, int commonSuffix, List<T> aList, int aDiff,
			List<T> bList, int bDiff, List<Action> actions, List<T> diffs) {
		final int matrixSize = (aDiff + 1) * (bDiff + 1);
		if (matrix.length < matrixSize) {
			matrix = new int[matrixSize];
		} else {
			Arrays.fill(matrix, 0, matrixSize, 0);
		}

		final int x = 1;
		final int y = aDiff + 1;
		final int z = y + 1;

		// Bottom up dynamic programming
		// https://www.ics.uci.edu/~eppstein/161/960229.html
		int l = (bDiff) * (aDiff + 1) - 2;
		for (int bPos = (commonPrefix + bDiff) - 1; bPos >= commonPrefix; bPos--) {
			for (int aPos = (commonPrefix + aDiff) - 1; aPos >= commonPrefix; aPos--) {
				if (aList.get(aPos).compareTo(bList.get(bPos)) == 0) {
					matrix[l] = matrix[l + z] + 1;
				} else {
					matrix[l] = Math.max(matrix[l + x], matrix[l + y]);
				}
				l--;
			}
			// skip additional zero column
			l--;
		}

		final int aSizeWithoutSuffix = aList.size() - commonSuffix;
		final int bSizeWithoutSuffix = bList.size() - commonSuffix;

		int take = 0;
		int add = 0;
		int skip = 0;
		int update = 0;

		int a, b;
		a = b = commonPrefix;
		l = 0;
		// The subsequence itself
		// https://www.ics.uci.edu/~eppstein/161/960229.html
		while (l < ((bDiff) * (aDiff + 1) - 1)) {
			if (aList.get(a).compareTo(bList.get(b)) == 0) {
				if (skip > 0) {
					update = Math.min(skip, add);
					if (update > 0) {
						actions.add(new Action(ActionType.UPDATE, update));
						skip -= update;
						add -= update;
					}
					if (skip > 0) {
						actions.add(new Action(ActionType.SKIP, skip));
					}
					skip = 0;
				}
				if (add > 0) {
					actions.add(new Action(ActionType.ADD, add));
					add = 0;
				}
				take++;
				l += z;
				a++;
				b++;
			} else {
				if (take > 0) {
					actions.add(new Action(ActionType.TAKE, take));
					take = 0;
				}

				if (matrix[l + x] > matrix[l + y]) {
					// skip a
					skip++;
					a++;
					l += x;
				} else {
					// take b
					add++;
					diffs.add(bList.get(b));
					b++;
					l += y;
				}
			}

			if (a >= aSizeWithoutSuffix || b >= bSizeWithoutSuffix) {
				break;
			}
		}

		if (take > 0) {
			actions.add(new Action(ActionType.TAKE, take));
			take = 0;
		}

		skip += aSizeWithoutSuffix - a;
		add += bSizeWithoutSuffix - b;
		for (; b < bSizeWithoutSuffix; b++) {
			diffs.add(bList.get(b));
		}

		if (skip > 0) {
			update = Math.min(skip, add);
			if (update > 0) {
				actions.add(new Action(ActionType.UPDATE, update));
				skip -= update;
				add -= update;
			}
			if (skip > 0) {
				actions.add(new Action(ActionType.SKIP, skip));
			}
			skip = 0;
		}
		if (add > 0) {
			actions.add(new Action(ActionType.ADD, add));
			add = 0;
		}
	}

}
