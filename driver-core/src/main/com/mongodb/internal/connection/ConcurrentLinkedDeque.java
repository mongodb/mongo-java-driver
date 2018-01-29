/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package com.mongodb.internal.connection;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A concurrent linked-list implementation of a {@link Deque} (double-ended queue).
 *
 * <p>This class should not be considered a part of the public API.</p>

 * <p>Concurrent insertion, removal, and access
 * operations execute safely across multiple threads. Iterators are
 * <i>weakly consistent</i>, returning elements reflecting the state
 * of the deque at some point at or since the creation of the
 * iterator.  They do <em>not</em> throw {@link
 * java.util.ConcurrentModificationException}, and may proceed concurrently with
 * other operations.
 *
 * <p>This class and its iterators implement all of the
 * <em>optional</em> methods of the {@link java.util.Collection} and {@link
 * java.util.Iterator} interfaces. Like most other concurrent collection
 * implementations, this class does not permit the use of
 * {@code null} elements.  because some null arguments and return
 * values cannot be reliably distinguished from the absence of
 * elements. Arbitrarily, the {@link java.util.Collection#remove} method is
 * mapped to {@code removeFirstOccurrence}, and {@link
 * java.util.Collection#add} is mapped to {@code addLast}.
 *
 * <p>Beware that, unlike in most collections, the {@code size}
 * method is <em>NOT</em> a constant-time operation. Because of the
 * asynchronous nature of these deques, determining the current number
 * of elements requires a traversal of the elements.
 *
 * <p>This class is {@code Serializable}, but relies on default
 * serialization mechanisms.  Usually, it is a better idea for any
 * serializable class using a {@code ConcurrentLinkedDeque} to instead
 * serialize a snapshot of the elements obtained by method
 * {@code toArray}.
 *
 * @author  Doug Lea
 * @param <E> the type of elements held in this collection
 */
public class ConcurrentLinkedDeque<E>
extends AbstractCollection<E>
implements Deque<E>, java.io.Serializable {

    // CHECKSTYLE:OFF

    /*
     * This is an adaptation of an algorithm described in Paul
     * Martin's "A Practical Lock-Free Doubly-Linked List". Sun Labs
     * Tech report. The basic idea is to primarily rely on
     * next-pointers to ensure consistency. Prev-pointers are in part
     * optimistic, reconstructed using forward pointers as needed.
     * The main forward list uses a variant of HM-list algorithm
     * similar to the one used in ConcurrentSkipListMap class, but a
     * little simpler.  It is also basically similar to the approach
     * in Edya Ladan-Mozes and Nir Shavit "An Optimistic Approach to
     * Lock-Free FIFO Queues" in DISC04.
     *
     * Quoting a summary in Paul Martin's tech report:
     *
     *  All cleanups work to maintain these invariants:
     *    (1) forward pointers are the ground truth.
     *    (2) forward pointers to dead nodes can be improved by swinging them
     *        further forward around the dead node.
     *    (2.1) forward pointers are still correct when pointing to dead
     *        nodes, and forward pointers from dead nodes are left
     *        as they were when the node was deleted.
     *    (2.2) multiple dead nodes may point forward to the same node.
     *    (3) backward pointers were correct when they were installed
     *    (3.1) backward pointers are correct when pointing to any
     *        node which points forward to them, but since more than
     *        one forward pointer may point to them, the live one is best.
     *    (4) backward pointers that are out of date due to deletion
     *        point to a deleted node, and need to point further back until
     *        they point to the live node that points to their source.
     *    (5) backward pointers that are out of date due to insertion
     *        point too far backwards, so shortening their scope (by searching
     *        forward) fixes them.
     *    (6) backward pointers from a dead node cannot be "improved" since
     *        there may be no live node pointing forward to their origin.
     *        (However, it does no harm to try to improve them while
     *        racing with a deletion.)
     *
     *
     * Notation guide for local variables
     *   n, b, f :  a node, its predecessor, and successor
     *   s       :  some other successor
     */

    /**
     * Linked Nodes. As a minor efficiency hack, this class
     * opportunistically inherits from AtomicReference, with the
     * atomic ref used as the "next" link.
     *
     * Nodes are in doubly-linked lists. There are three
     * kinds of special nodes, distinguished by:
     *  * The list header   has a null prev link.
     *  * The list trailer  has a null next link.
     *  * A deletion marker has a prev link pointing to itself.
     * All three kinds of special nodes have null element fields.
     *
     * Regular nodes have non-null element, next, and prev fields.  To
     * avoid visible inconsistencies when deletions overlap element
     * replacement, replacements are done by replacing the node, not
     * just setting the element.
     *
     * Nodes can be traversed by read-only ConcurrentLinkedDeque class
     * operations just by following raw next pointers, so long as they
     * ignore any special nodes seen along the way. (This is automated
     * in method forward.)  However, traversal using prev pointers is
     * not guaranteed to see all live nodes since a prev pointer of a
     * deleted node can become unrecoverably stale.
     */
    static final class Node<E> extends AtomicReference<Node<E>> {
        private volatile Node<E> prev;
        final E element;
        private static final long serialVersionUID = 876323262645176354L;

        /** Creates a node with given contents. */
        Node(E element, Node<E> next, Node<E> prev) {
            super(next);
            this.prev = prev;
            this.element = element;
        }

        /** Creates a marker node with given successor. */
        Node(Node<E> next) {
            super(next);
            this.prev = this;
            this.element = null;
        }

        /**
         * Gets next link (which is actually the value held
         * as atomic reference).
         */
        private Node<E> getNext() {
            return get();
        }

        /**
         * Sets next link.
         * @param n the next node
         */
        void setNext(Node<E> n) {
            set(n);
        }

        /**
         * compareAndSet next link
         */
        private boolean casNext(Node<E> cmp, Node<E> val) {
            return compareAndSet(cmp, val);
        }

        /**
         * Gets prev link.
         */
        private Node<E> getPrev() {
            return prev;
        }

        /**
         * Sets prev link.
         * @param b the previous node
         */
        void setPrev(Node<E> b) {
            prev = b;
        }

        /**
         * Returns true if this is a header, trailer, or marker node.
         */
        boolean isSpecial() {
            return element == null;
        }

        /**
         * Returns true if this is a trailer node.
         */
        boolean isTrailer() {
            return getNext() == null;
        }

        /**
         * Returns true if this is a header node.
         */
        boolean isHeader() {
            return getPrev() == null;
        }

        /**
         * Returns true if this is a marker node.
         */
        boolean isMarker() {
            return getPrev() == this;
        }

        /**
         * Returns true if this node is followed by a marker node,
         * meaning that this node is deleted.
         *
         * @return true if this node is deleted
         */
        boolean isDeleted() {
            Node<E> f = getNext();
            return f != null && f.isMarker();
        }

        /**
         * Returns next node, ignoring deletion marker.
         */
        private Node<E> nextNonmarker() {
            Node<E> f = getNext();
            return (f == null || !f.isMarker()) ? f : f.getNext();
        }

        /**
         * Returns the next non-deleted node, swinging next pointer
         * around any encountered deleted nodes, and also patching up
         * successor's prev link to point back to this.  Returns
         * null if this node is trailer so has no successor.
         *
         * @return successor, or null if no such
         */
        Node<E> successor() {
            Node<E> f = nextNonmarker();
            for (;;) {
                if (f == null)
                    return null;
                if (!f.isDeleted()) {
                    if (f.getPrev() != this && !isDeleted())
                        f.setPrev(this);       // relink f's prev
                    return f;
                }
                Node<E> s = f.nextNonmarker();
                if (f == getNext())
                    casNext(f, s);             // unlink f
                f = s;
            }
        }

        /**
         * Returns the apparent predecessor of target by searching
         * forward for it, starting at this node, patching up pointers
         * while traversing. Used by predecessor().
         *
         * @return target's predecessor, or null if not found
         */
        private Node<E> findPredecessorOf(Node<E> target) {
            Node<E> n = this;
            for (;;) {
                Node<E> f = n.successor();
                if (f == target)
                    return n;
                if (f == null)
                    return null;
                n = f;
            }
        }

        /**
         * Returns the previous non-deleted node, patching up pointers
         * as needed.  Returns null if this node is header so has no
         * successor.  May also return null if this node is deleted,
         * so doesn't have a distinct predecessor.
         *
         * @return predecessor, or null if not found
         */
        Node<E> predecessor() {
            Node<E> n = this;
            for (;;) {
                Node<E> b = n.getPrev();
                if (b == null)
                    return n.findPredecessorOf(this);
                Node<E> s = b.getNext();
                if (s == this)
                    return b;
                if (s == null || !s.isMarker()) {
                    Node<E> p = b.findPredecessorOf(this);
                    if (p != null)
                        return p;
                }
                n = b;
            }
        }

        /**
         * Returns the next node containing a nondeleted user element.
         * Use for forward list traversal.
         *
         * @return successor, or null if no such
         */
        Node<E> forward() {
            Node<E> f = successor();
            return (f == null || f.isSpecial()) ? null : f;
        }

        /**
         * Returns previous node containing a nondeleted user element, if
         * possible.  Use for backward list traversal, but beware that
         * if this method is called from a deleted node, it might not
         * be able to determine a usable predecessor.
         *
         * @return predecessor, or null if no such could be found
         */
        Node<E> back() {
            Node<E> f = predecessor();
            return (f == null || f.isSpecial()) ? null : f;
        }

        /**
         * Tries to insert a node holding element as successor, failing
         * if this node is deleted.
         *
         * @param element the element
         * @return the new node, or null on failure
         */
        Node<E> append(E element) {
            for (;;) {
                Node<E> f = getNext();
                if (f == null || f.isMarker())
                    return null;
                Node<E> x = new Node<E>(element, f, this);
                if (casNext(f, x)) {
                    f.setPrev(x); // optimistically link
                    return x;
                }
            }
        }

        /**
         * Tries to insert a node holding element as predecessor, failing
         * if no live predecessor can be found to link to.
         *
         * @param element the element
         * @return the new node, or null on failure
         */
        Node<E> prepend(E element) {
            for (;;) {
                Node<E> b = predecessor();
                if (b == null)
                    return null;
                Node<E> x = new Node<E>(element, this, b);
                if (b.casNext(this, x)) {
                    setPrev(x); // optimistically link
                    return x;
                }
            }
        }

        /**
         * Tries to mark this node as deleted, failing if already
         * deleted or if this node is header or trailer.
         *
         * @return true if successful
         */
        boolean delete() {
            Node<E> b = getPrev();
            Node<E> f = getNext();
            if (b != null && f != null && !f.isMarker() &&
                casNext(f, new Node<E>(f))) {
                if (b.casNext(this, f))
                    f.setPrev(b);
                return true;
            }
            return false;
        }

        /**
         * Tries to insert a node holding element to replace this node.
         * failing if already deleted.  A currently unused proof of
         * concept that demonstrates atomic node content replacement.
         *
         * Although this implementation ensures that exactly one
         * version of this Node is alive at a given time, it fails to
         * maintain atomicity in the sense that iterators may
         * encounter both the old and new versions of the element.
         *
         * @param newElement the new element
         * @return the new node, or null on failure
         */
        Node<E> replace(E newElement) {
            for (;;) {
                Node<E> b = getPrev();
                Node<E> f = getNext();
                if (b == null || f == null || f.isMarker())
                    return null;
                Node<E> x = new Node<E>(newElement, f, b);
                if (casNext(f, new Node<E>(x))) {
                    b.successor(); // to relink b
                    x.successor(); // to relink f
                    return x;
                }
            }
        }
    }

    // Minor convenience utilities

    /**
     * Returns true if given reference is non-null and isn't a header,
     * trailer, or marker.
     *
     * @param n (possibly null) node
     * @return true if n exists as a user node
     */
    private static boolean usable(Node<?> n) {
        return n != null && !n.isSpecial();
    }

    /**
     * Throws NullPointerException if argument is null.
     *
     * @param v the element
     */
    private static void checkNotNull(Object v) {
        if (v == null)
            throw new NullPointerException();
    }

    /**
     * Returns element unless it is null, in which case throws
     * NoSuchElementException.
     *
     * @param v the element
     * @return the element
     */
    private E screenNullResult(E v) {
        if (v == null)
            throw new NoSuchElementException();
        return v;
    }

    /**
     * Creates an array list and fills it with elements of this list.
     * Used by toArray.
     *
     * @return the array list
     */
    private ArrayList<E> toArrayList() {
        ArrayList<E> c = new ArrayList<E>();
        for (Node<E> n = header.forward(); n != null; n = n.forward())
            c.add(n.element);
        return c;
    }

    // Fields and constructors

    private static final long serialVersionUID = 876323262645176354L;

    /**
     * List header. First usable node is at header.forward().
     */
    private final Node<E> header;

    /**
     * List trailer. Last usable node is at trailer.back().
     */
    private final Node<E> trailer;

    /**
     * Constructs an empty deque.
     */
    public ConcurrentLinkedDeque() {
        Node<E> h = new Node<E>(null, null, null);
        Node<E> t = new Node<E>(null, null, h);
        h.setNext(t);
        header = h;
        trailer = t;
    }

    /**
     * Constructs a deque initially containing the elements of
     * the given collection, added in traversal order of the
     * collection's iterator.
     *
     * @param c the collection of elements to initially contain
     * @throws NullPointerException if the specified collection or any
     *         of its elements are null
     */
    public ConcurrentLinkedDeque(Collection<? extends E> c) {
        this();
        addAll(c);
    }

    /**
     * Inserts the specified element at the front of this deque.
     *
     * @throws NullPointerException {@inheritDoc}
     */
    public void addFirst(E e) {
        checkNotNull(e);
        while (header.append(e) == null)
            ;
    }

    /**
     * Inserts the specified element at the end of this deque.
     * This is identical in function to the {@code add} method.
     *
     * @throws NullPointerException {@inheritDoc}
     */
    public void addLast(E e) {
        checkNotNull(e);
        while (trailer.prepend(e) == null)
            ;
    }

    /**
     * Inserts the specified element at the front of this deque.
     *
     * @return {@code true} always
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean offerFirst(E e) {
        addFirst(e);
        return true;
    }

    /**
     * Inserts the specified element at the end of this deque.
     *
     * <p>This method is equivalent to {@link #add}.
     *
     * @return {@code true} always
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean offerLast(E e) {
        addLast(e);
        return true;
    }

    public E peekFirst() {
        Node<E> n = header.successor();
        return (n == null) ? null : n.element;
    }

    public E peekLast() {
        Node<E> n = trailer.predecessor();
        return (n == null) ? null : n.element;
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public E getFirst() {
        return screenNullResult(peekFirst());
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public E getLast() {
        return screenNullResult(peekLast());
    }

    public E pollFirst() {
        for (;;) {
            Node<E> n = header.successor();
            if (!usable(n))
                return null;
            if (n.delete())
                return n.element;
        }
    }

    public E pollLast() {
        for (;;) {
            Node<E> n = trailer.predecessor();
            if (!usable(n))
                return null;
            if (n.delete())
                return n.element;
        }
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public E removeFirst() {
        return screenNullResult(pollFirst());
    }

    /**
     * @throws NoSuchElementException {@inheritDoc}
     */
    public E removeLast() {
        return screenNullResult(pollLast());
    }

    // *** Queue and stack methods ***

    /**
     * Inserts the specified element at the tail of this queue.
     *
     * @return {@code true}
     *   (as specified by {@link java.util.Queue#offer Queue.offer})
     * @throws NullPointerException if the specified element is null
     */
    public boolean offer(E e) {
        return offerLast(e);
    }

    /**
     * Inserts the specified element at the tail of this deque.
     *
     * @return {@code true} (as specified by {@link Collection#add})
     * @throws NullPointerException if the specified element is null
     */
    public boolean add(E e) {
        return offerLast(e);
    }

    public E poll()           { return pollFirst(); }
    public E remove()         { return removeFirst(); }
    public E peek()           { return peekFirst(); }
    public E element()        { return getFirst(); }
    public void push(E e)     { addFirst(e); }
    public E pop()            { return removeFirst(); }

    /**
     * Removes the first element {@code e} such that
     * {@code o.equals(e)}, if such an element exists in this deque.
     * If the deque does not contain the element, it is unchanged.
     *
     * @param o element to be removed from this deque, if present
     * @return {@code true} if the deque contained the specified element
     * @throws NullPointerException if the specified element is {@code null}
     */
    public boolean removeFirstOccurrence(Object o) {
        checkNotNull(o);
        for (;;) {
            Node<E> n = header.forward();
            for (;;) {
                if (n == null)
                    return false;
                if (o.equals(n.element)) {
                    if (n.delete())
                        return true;
                    else
                        break; // restart if interference
                }
                n = n.forward();
            }
        }
    }

    /**
     * Removes the last element {@code e} such that
     * {@code o.equals(e)}, if such an element exists in this deque.
     * If the deque does not contain the element, it is unchanged.
     *
     * @param o element to be removed from this deque, if present
     * @return {@code true} if the deque contained the specified element
     * @throws NullPointerException if the specified element is {@code null}
     */
    public boolean removeLastOccurrence(Object o) {
        checkNotNull(o);
        for (;;) {
            Node<E> s = trailer;
            for (;;) {
                Node<E> n = s.back();
                if (s.isDeleted() || (n != null && n.successor() != s))
                    break; // restart if pred link is suspect.
                if (n == null)
                    return false;
                if (o.equals(n.element)) {
                    if (n.delete())
                        return true;
                    else
                        break; // restart if interference
                }
                s = n;
            }
        }
    }

    /**
     * Returns {@code true} if this deque contains at least one
     * element {@code e} such that {@code o.equals(e)}.
     *
     * @param o element whose presence in this deque is to be tested
     * @return {@code true} if this deque contains the specified element
     */
    public boolean contains(Object o) {
        if (o == null) return false;
        for (Node<E> n = header.forward(); n != null; n = n.forward())
            if (o.equals(n.element))
                return true;
        return false;
    }

    /**
     * Returns {@code true} if this collection contains no elements.
     *
     * @return {@code true} if this collection contains no elements
     */
    public boolean isEmpty() {
        return !usable(header.successor());
    }

    /**
     * Returns the number of elements in this deque.  If this deque
     * contains more than {@code Integer.MAX_VALUE} elements, it
     * returns {@code Integer.MAX_VALUE}.
     *
     * <p>Beware that, unlike in most collections, this method is
     * <em>NOT</em> a constant-time operation. Because of the
     * asynchronous nature of these deques, determining the current
     * number of elements requires traversing them all to count them.
     * Additionally, it is possible for the size to change during
     * execution of this method, in which case the returned result
     * will be inaccurate. Thus, this method is typically not very
     * useful in concurrent applications.
     *
     * @return the number of elements in this deque
     */
    public int size() {
        long count = 0;
        for (Node<E> n = header.forward(); n != null; n = n.forward())
            ++count;
        return (count >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) count;
    }

    /**
     * Removes the first element {@code e} such that
     * {@code o.equals(e)}, if such an element exists in this deque.
     * If the deque does not contain the element, it is unchanged.
     *
     * @param o element to be removed from this deque, if present
     * @return {@code true} if the deque contained the specified element
     * @throws NullPointerException if the specified element is {@code null}
     */
    public boolean remove(Object o) {
        return removeFirstOccurrence(o);
    }

    /**
     * Appends all of the elements in the specified collection to the end of
     * this deque, in the order that they are returned by the specified
     * collection's iterator.  The behavior of this operation is undefined if
     * the specified collection is modified while the operation is in
     * progress.  (This implies that the behavior of this call is undefined if
     * the specified Collection is this deque, and this deque is nonempty.)
     *
     * @param c the elements to be inserted into this deque
     * @return {@code true} if this deque changed as a result of the call
     * @throws NullPointerException if {@code c} or any element within it
     * is {@code null}
     */
    public boolean addAll(Collection<? extends E> c) {
        Iterator<? extends E> it = c.iterator();
        if (!it.hasNext())
            return false;
        do {
            addLast(it.next());
        } while (it.hasNext());
        return true;
    }

    /**
     * Removes all of the elements from this deque.
     */
    public void clear() {
        while (pollFirst() != null)
            ;
    }

    /**
     * Returns an array containing all of the elements in this deque, in
     * proper sequence (from first to last element).
     *
     * <p>The returned array will be "safe" in that no references to it are
     * maintained by this deque.  (In other words, this method must allocate
     * a new array).  The caller is thus free to modify the returned array.
     *
     * <p>This method acts as bridge between array-based and collection-based
     * APIs.
     *
     * @return an array containing all of the elements in this deque
     */
    public Object[] toArray() {
        return toArrayList().toArray();
    }

    /**
     * Returns an array containing all of the elements in this deque,
     * in proper sequence (from first to last element); the runtime
     * type of the returned array is that of the specified array.  If
     * the deque fits in the specified array, it is returned therein.
     * Otherwise, a new array is allocated with the runtime type of
     * the specified array and the size of this deque.
     *
     * <p>If this deque fits in the specified array with room to spare
     * (i.e., the array has more elements than this deque), the element in
     * the array immediately following the end of the deque is set to
     * {@code null}.
     *
     * <p>Like the {@link #toArray()} method, this method acts as bridge between
     * array-based and collection-based APIs.  Further, this method allows
     * precise control over the runtime type of the output array, and may,
     * under certain circumstances, be used to save allocation costs.
     *
     * <p>Suppose {@code x} is a deque known to contain only strings.
     * The following code can be used to dump the deque into a newly
     * allocated array of {@code String}:
     *
     * <pre>
     *     String[] y = x.toArray(new String[0]);</pre>
     *
     * Note that {@code toArray(new Object[0])} is identical in function to
     * {@code toArray()}.
     *
     * @param a the array into which the elements of the deque are to
     *          be stored, if it is big enough; otherwise, a new array of the
     *          same runtime type is allocated for this purpose
     * @return an array containing all of the elements in this deque
     * @throws ArrayStoreException if the runtime type of the specified array
     *         is not a supertype of the runtime type of every element in
     *         this deque
     * @throws NullPointerException if the specified array is null
     */
    public <T> T[] toArray(T[] a) {
        return toArrayList().toArray(a);
    }

    /**
     * Returns an iterator over the elements in this deque in proper sequence.
     * The elements will be returned in order from first (head) to last (tail).
     * The returned {@code Iterator} is a "weakly consistent" iterator that
     * will never throw {@link java.util.ConcurrentModificationException
     * ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     *
     * @return an iterator over the elements in this deque in proper sequence
     */
    public RemovalReportingIterator<E> iterator() {
        return new CLDIterator();
    }

    final class CLDIterator implements RemovalReportingIterator<E> {
        Node<E> last;
        Node<E> next = header.forward();

        public boolean hasNext() {
            return next != null;
        }

        public E next() {
            Node<E> l = last = next;
            if (l == null)
                throw new NoSuchElementException();
            next = next.forward();
            return l.element;
        }

        public void remove() {
            reportingRemove();
        }

        @Override
        public boolean reportingRemove() {
            Node<E> l = last;
            if (l == null)
                throw new IllegalStateException();
            boolean successfullyRemoved = l.delete();
            while (!successfullyRemoved && !l.isDeleted()) {
                successfullyRemoved = l.delete();
            }
            return successfullyRemoved;
        }
    }

    /**
     * Not yet implemented.
     */
    public Iterator<E> descendingIterator() {
        throw new UnsupportedOperationException();
    }

    public interface RemovalReportingIterator<E> extends Iterator<E> {
        /**
         * Removes from the underlying collection the last element returned by this iterator and reports whether the current element was
         * removed by the call.  This method can be called only once per call to {@link #next}.
         *
         * @return true if the element was successfully removed by this call, false if the element had already been removed by a concurrent
         * removal
         * @throws IllegalStateException if the {@code next} method has not
         *         yet been called, or the {@code remove} method has already
         *         been called after the last call to the {@code next}
         *         method
         */
        boolean reportingRemove();
    }
}
