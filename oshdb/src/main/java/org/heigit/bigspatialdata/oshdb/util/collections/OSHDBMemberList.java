package org.heigit.bigspatialdata.oshdb.util.collections;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import org.heigit.bigspatialdata.oshdb.OSHDBMember;
import it.unimi.dsi.fastutil.objects.AbstractObjectList;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.objects.ObjectListIterator;

public class OSHDBMemberList extends AbstractObjectList<OSHDBMember> implements RandomAccess, Cloneable {
  private static final OSHDBMember[] EMPTY = new OSHDBMember[0];
  private static final int DEFAULT_INITIAL_CAPACITY = 16;

  protected OSHDBMember a[] = EMPTY;

  protected int size;

  protected OSHDBMemberList(final OSHDBMember a[], @SuppressWarnings("unused") boolean dummy) {
    this.a = a;
  }

  public OSHDBMemberList(final int capacity) {
    if (capacity < 0)
      throw new IllegalArgumentException("Initial capacity (" + capacity + ") is negative");
    a = new OSHDBMember[capacity];
  }

  public OSHDBMemberList() {}

  public OSHDBMemberList(final OSHDBMember a[]) {
    this(a, 0, a.length);
  }

  public OSHDBMemberList(final OSHDBMember a[], final int offset, final int length) {
    this(length);
    System.arraycopy(a, offset, this.a, 0, length);
    size = length;
  }

  public OSHDBMemberList(final Iterator<? extends OSHDBMember> i) {
    this();
    while (i.hasNext())
      this.add((i.next()));
  }

  public OSHDBMember[] elements() {
    return a;
  }

  public static OSHDBMemberList wrap(final OSHDBMember a[], final int length) {
    if (length > a.length)
      throw new IllegalArgumentException("The specified length (" + length
          + ") is greater than the array size (" + a.length + ")");
    final OSHDBMemberList l = new OSHDBMemberList(a, false);
    l.size = length;
    return l;
  }

  public static OSHDBMemberList wrap(final OSHDBMember a[]) {
    return wrap(a, a.length);
  }

  public void ensureCapacity(final int capacity) {
    if (capacity > a.length) {
      final OSHDBMember t[] = new OSHDBMember[capacity];
      System.arraycopy(a, 0, t, 0, size);
      a = t;
    }
  }

  private void grow(final int capacity) {
    if (capacity > a.length) {
      final int newLength;
      if (a.length == 0) {
        newLength = DEFAULT_INITIAL_CAPACITY;
      } else {
        newLength = (int) Math
            .max(Math.min(2L * a.length, it.unimi.dsi.fastutil.Arrays.MAX_ARRAY_SIZE), capacity);
      }
      final OSHDBMember t[] = new OSHDBMember[newLength];
      System.arraycopy(a, 0, t, 0, size);
      a = t;
    }
  }

  @Override
  public void add(final int index, final OSHDBMember k) {
    ensureIndex(index);
    grow(size + 1);
    if (index != size)
      System.arraycopy(a, index, a, index + 1, size - index);
    a[index] = k;
    size++;
  }

  @Override
  public boolean add(final OSHDBMember k) {
    grow(size + 1);
    a[size++] = k;
    return true;
  }

  public void set(Iterable<OSHDBMember> other) {
    if (other instanceof OSHDBMemberList) {
      set((OSHDBMemberList) other);
      return;
    }
    clear();
    if (other instanceof Collection<?>) {
      int newSize = ((Collection<?>) other).size();
      ensureCapacity(newSize);
      int i = 0;
      for (OSHDBMember t : other) {
        a[i++] = t;
      }
      size = newSize;
    } else {
      other.forEach(this::add);
    }
  }

  public void set(OSHDBMemberList other) {
    if (a.length < other.size) {
      final OSHDBMember[] t = new OSHDBMember[other.size];
      a = t;
    } else {
      clear();
    }
    System.arraycopy(other.a, 0, a, 0, other.size);
    size = other.size;
  }

  @Override
  public OSHDBMember get(final int index) {
    if (index >= size)
      throw new IndexOutOfBoundsException(
          "Index (" + index + ") is greater than or equal to list size (" + size + ")");
    return a[index];
  }

  @Override
  public int indexOf(final Object k) {
    for (int i = 0; i < size; i++)
      if (java.util.Objects.equals(k, a[i]))
        return i;
    return -1;
  }

  @Override
  public int lastIndexOf(final Object k) {
    for (int i = size; i-- != 0;)
      if (java.util.Objects.equals(k, a[i]))
        return i;
    return -1;
  }

  @Override
  public OSHDBMember remove(final int index) {
    if (index >= size)
      throw new IndexOutOfBoundsException(
          "Index (" + index + ") is greater than or equal to list size (" + size + ")");
    final OSHDBMember old = a[index];
    size--;
    if (index != size)
      System.arraycopy(a, index + 1, a, index, size - index);
    a[size] = null;
    return old;
  }

  @Override
  public boolean remove(final Object k) {
    int index = indexOf(k);
    if (index == -1)
      return false;
    remove(index);
    return true;
  }

  @Override
  public OSHDBMember set(final int index, final OSHDBMember k) {
    if (index >= size)
      throw new IndexOutOfBoundsException(
          "Index (" + index + ") is greater than or equal to list size (" + size + ")");
    OSHDBMember old = a[index];
    a[index] = k;
    return old;
  }

  @Override
  public void clear() {
    Arrays.fill(a, 0, size, null);
    size = 0;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void size(final int size) {
    if (size > a.length)
      ensureCapacity(size);
    if (size > this.size)
      Arrays.fill(a, this.size, size, (null));
    else
      Arrays.fill(a, size, this.size, (null));
    this.size = size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }


  public void trim() {
    trim(0);
  }

  public void trim(final int n) {
    if (n >= a.length || size == a.length)
      return;
    final OSHDBMember t[] = new OSHDBMember[Math.max(n, size)];
    a = t;
  }

  /**
   * Copies element of this type-specific list into the given array using optimized system calls.
   *
   * @param from the start index (inclusive).
   * @param a the destination array.
   * @param offset the offset into the destination array where to store the first element copied.
   * @param length the number of elements to be copied.
   */
  @Override
  public void getElements(final int from, final Object[] a, final int offset, final int length) {
    ObjectArrays.ensureOffsetLength(a, offset, length);
    System.arraycopy(this.a, from, a, offset, length);
  }

  @Override
  public void removeElements(final int from, final int to) {
    it.unimi.dsi.fastutil.Arrays.ensureFromTo(size, from, to);
    System.arraycopy(a, to, a, from, size - to);
    size -= (to - from);
    int i = to - from;
    while (i-- != 0)
      a[size + i] = null;
  }

  @Override
  public void addElements(final int index, final OSHDBMember a[], final int offset, final int length) {
    ensureIndex(index);
    ObjectArrays.ensureOffsetLength(a, offset, length);
    grow(size + length);
    System.arraycopy(this.a, index, this.a, index + length, size - index);
    System.arraycopy(a, offset, this.a, index, length);
    size += length;
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    final Object[] a = this.a;
    int j = 0;
    for (int i = 0; i < size; i++)
      if (!c.contains((a[i])))
        a[j++] = a[i];
    Arrays.fill(a, j, size, null);
    final boolean modified = size != j;
    size = j;
    return modified;
  }

  @Override
  public ObjectListIterator<OSHDBMember> listIterator(final int index) {
    ensureIndex(index);
    return new ObjectListIterator<OSHDBMember>() {
      int pos = index, last = -1;

      @Override
      public boolean hasNext() {
        return pos < size;
      }

      @Override
      public boolean hasPrevious() {
        return pos > 0;
      }

      @Override
      public OSHDBMember next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return a[last = pos++];
      }

      @Override
      public OSHDBMember previous() {
        if (!hasPrevious())
          throw new NoSuchElementException();
        return a[last = --pos];
      }

      @Override
      public int nextIndex() {
        return pos;
      }

      @Override
      public int previousIndex() {
        return pos - 1;
      }

      @Override
      public void add(OSHDBMember k) {
        OSHDBMemberList.this.add(pos++, k);
        last = -1;
      }

      @Override
      public void set(OSHDBMember k) {
        if (last == -1)
          throw new IllegalStateException();
        OSHDBMemberList.this.set(last, k);
      }

      @Override
      public void remove() {
        if (last == -1)
          throw new IllegalStateException();
        OSHDBMemberList.this.remove(last);
        /*
         * If the last operation was a next(), we are removing an element *before* us, and we must
         * decrease pos correspondingly.
         */
        if (last < pos)
          pos--;
        last = -1;
      }
    };
  }

  @Override
  public OSHDBMemberList clone() {
    OSHDBMemberList c = new OSHDBMemberList(size);
    System.arraycopy(a, 0, c.a, 0, size);
    c.size = size;
    return c;
  }

  private boolean valEquals(final OSHDBMember a, final OSHDBMember b) {
    return a == null ? b == null : a.equals(b);
  }

  public boolean equals(final OSHDBMemberList l) {
    if (l == this)
      return true;
    int s = size();
    if (s != l.size())
      return false;
    final OSHDBMember[] a1 = a;
    final OSHDBMember[] a2 = l.a;
    while (s-- != 0)
      if (!valEquals(a1[s], a2[s]))
        return false;
    return true;
  }

  public int compareTo(final OSHDBMemberList l) {
    final int s1 = size(), s2 = l.size();
    final OSHDBMember a1[] = a, a2[] = l.a;
    OSHDBMember e1, e2;
    int r, i;
    for (i = 0; i < s1 && i < s2; i++) {
      e1 = a1[i];
      e2 = a2[i];
      if ((r = e1.compareTo(e2)) != 0)
        return r;
    }
    return i < s2 ? -1 : (i < s1 ? 1 : 0);
  }

  private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
    s.defaultWriteObject();
    for (int i = 0; i < size; i++)
      s.writeObject(a[i]);
  }

  private void readObject(java.io.ObjectInputStream s)
      throws java.io.IOException, ClassNotFoundException {
    s.defaultReadObject();
    a = (OSHDBMember[]) new Object[size];
    for (int i = 0; i < size; i++)
      a[i] = (OSHDBMember) s.readObject();
  }

}
