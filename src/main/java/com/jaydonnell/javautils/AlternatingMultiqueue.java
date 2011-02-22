package com.jaydonnell.javautils;

import com.google.common.collect.LinkedListMultimap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.*;
import net.jcip.annotations.*;

/** 
 * @author ddonnell
 */
public class AlternatingMultiqueue<K,E> {

    private final int capacity;
    private final AtomicInteger count = new AtomicInteger(0);

    // TODO is LinkedListMultimap the right choice?
    private final LinkedListMultimap<K, E> multiMap = LinkedListMultimap.create();
    private final LinkedList<K> keys = new LinkedList<K>();
    private final AtomicInteger currentKey = new AtomicInteger(0);

    /** Lock held by take, poll, etc */
    private final ReentrantLock takeLock = new ReentrantLock();

    /** Wait queue for waiting takes */
    private final Condition notEmpty = takeLock.newCondition();

    /** Lock held by put, offer, etc */
    private final ReentrantLock putLock = new ReentrantLock();

    /** Wait queue for waiting puts */
    private final Condition notFull = putLock.newCondition();

    private final ReentrantLock queueChangeLock = new ReentrantLock();

    /**
     * Signals a waiting take. Called only from put/offer (which do not
     * otherwise ordinarily lock takeLock.)
     */
    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * Signals a waiting put. Called only from take/poll.
     */
    private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }

    private synchronized void incrementCurrentKey() {
        for(;;) {
          int current = currentKey.get();
          int mod = keys.size() > 0 ? keys.size() : 1; 
          int next = (current + 1) % mod;
          if(currentKey.compareAndSet(current, next))
              return;
       }
        // int count = currentKey.incrementAndGet();
        // if (count >= keys.size())
        //    currentKey = 0;
    }

    private synchronized void decrementCurrentKey() { // hmmm
        for(;;) {
            int current = currentKey.get();
            int next = (current - 1); 
            if(currentKey.compareAndSet(current, next))
                return;
        }
    }

    // private void insert(K key, E e) {
    //     // xx
    //     multiMap.put(key, e);
    //     if(!keys.contains(key))
    //         keys.add(key);
    // }


    /**
     * @param x the item
     */
    // @GuardedBy("putLock")
    @GuardedBy("queueChangeLock")
    private void enqueue(K key, E e) {
        multiMap.put(key, e);
        if(!keys.contains(key))
            keys.add(key);
    }

    /**
     * Removes a node from head of queue.
     * @return the node
     */
    // @GuardedBy("takeLock")
    @GuardedBy("queueChangeLock")
    private E dequeue() {
        K key = keys.get(currentKey.get());
        E x = multiMap.get(key).remove(0);

        // - if key is empty after extract we need to remove key from keys
        // TODO do we need to remove it from multimap?
        if (multiMap.get(key).isEmpty()) {
            keys.remove(key);
            decrementCurrentKey();
            //currentKey -= 1; // atomic int?
        }
        // - we need to increment currentKey
        incrementCurrentKey();
        return x;
    }


    /**
     * Lock to prevent both puts and takes.
     */
    void fullyLock() {
        putLock.lock();
        takeLock.lock();
    }

    /**
     * Unlock to allow both puts and takes.
     */
    void fullyUnlock() {
        takeLock.unlock();
        putLock.unlock();
    }

    /**
     * Tells whether both locks are held by current thread.
     */
    boolean isFullyLocked() {
        return (putLock.isHeldByCurrentThread() &&
                takeLock.isHeldByCurrentThread());
    }

    public AlternatingMultiqueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }
        this.capacity = capacity;
    }

    /**
     * Returns the number of elements in this queue.
     *
     * @return the number of elements in this queue
     */
    public int size() {
        return count.get();
    }

    /**
     * Returns the number of additional elements that this queue can ideally
     * (in the absence of memory or resource constraints) accept without
     * blocking. This is always equal to the initial capacity of this queue
     * less the current <tt>size</tt> of this queue.
     *
     * <p>Note that you <em>cannot</em> always tell if an attempt to insert
     * an element will succeed by inspecting <tt>remainingCapacity</tt>
     * because it may be the case that another thread is about to
     * insert or remove an element.
     */
    public int remainingCapacity() {
        return capacity - count.get();
    }

    /**
     * Inserts the specified element at the tail of this queue, waiting if
     * necessary for space to become available.
     *
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    public void put(K key, E e) throws InterruptedException {
        if (e == null) throw new NullPointerException();
        // Note: convention in all put/take/etc is to preset local var
        // holding count negative to indicate failure unless set.
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        putLock.lockInterruptibly();
        try {
            /*
             * Note that count is used in wait guard even though it is
             * not protected by lock. This works because count can
             * only decrease at this point (all other puts are shut
             * out by lock), and we (or some other waiting put) are
             * signalled if it ever changes from
             * capacity. Similarly for all other uses of count in
             * other wait guards.
             */
            while (count.get() == capacity) { 
                    notFull.await();
            }

            final ReentrantLock queueChangeLock = this.queueChangeLock;
            queueChangeLock.lockInterruptibly();
            try {
                enqueue(key, e);
                c = count.getAndIncrement();
                if (c + 1 < capacity)
                    notFull.signal();
            } finally {
                queueChangeLock.unlock();
            }

        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
    }

    /**
     * Inserts the specified element at the tail of this queue, waiting if
     * necessary up to the specified wait time for space to become available.
     *
     * @return <tt>true</tt> if successful, or <tt>false</tt> if
     *         the specified waiting time elapses before space is available.
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean offer(K k, E e, long timeout, TimeUnit unit)
        throws InterruptedException {

        if (e == null) throw new NullPointerException();
        long nanos = unit.toNanos(timeout);
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        putLock.lockInterruptibly();
        try {
            while (count.get() == capacity) {
                if (nanos <= 0)
                    return false;
                nanos = notFull.awaitNanos(nanos);
            }

            final ReentrantLock queueChangeLock = this.queueChangeLock;
            queueChangeLock.lockInterruptibly();
            try {
                enqueue(k, e);
                c = count.getAndIncrement();
                if (c + 1 < capacity)
                    notFull.signal();
            } finally {
                queueChangeLock.unlock();
            }

        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
        return true;
    }


    /**
     * Inserts the specified element at the tail of this queue if it is
     * possible to do so immediately without exceeding the queue's capacity,
     * returning <tt>true</tt> upon success and <tt>false</tt> if this queue
     * is full.
     * When using a capacity-restricted queue, this method is generally
     * preferable to method {@link BlockingQueue#add add}, which can fail to
     * insert an element only by throwing an exception.
     *
     * @throws NullPointerException if the specified element is null
     */
    public boolean offer(K k, E e) {
        if (e == null) throw new NullPointerException();
        final AtomicInteger count = this.count;
        if (count.get() == capacity)
            return false;
        int c = -1;
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            if (count.get() < capacity) {

                final ReentrantLock queueChangeLock = this.queueChangeLock;
                queueChangeLock.lockInterruptibly();
                try {
                    enqueue(k, e);
                    c = count.getAndIncrement();
                    if (c + 1 < capacity)
                        notFull.signal();
                } finally {
                    queueChangeLock.unlock();
                }

            }
        } catch (InterruptedException ex) {
            return c >= 0; // hmm not sure?
        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
        return c >= 0;
    }

    /**
     * Retrieves and removes the head of this queue, waiting if no
     * elements are present on this queue.
     **/
    public E take() throws InterruptedException {
        E x;
        int c = -1;
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
               notEmpty.await();
            }

            final ReentrantLock queueChangeLock = this.queueChangeLock;
            queueChangeLock.lockInterruptibly();
            try {
                x = dequeue();
                c = count.getAndDecrement();
                if (c > 1)
                  notEmpty.signal();
            } finally {
                queueChangeLock.unlock();
            }

        } finally {
            takeLock.unlock();
        }
        if (c == capacity)
            signalNotFull();
        return x;
    }


    /**
     * Retrieves and removes the head of this queue, waiting if
     * necessary up to the specified wait time if no elements are
     * present on this queue.
     **/
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E x = null;
        int c = -1;
        long nanos = unit.toNanos(timeout);
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
                while (count.get() == 0) { 
                  if (nanos <= 0)
                    return null;
                  nanos = notEmpty.awaitNanos(nanos);
                }

                final ReentrantLock queueChangeLock = this.queueChangeLock;
                queueChangeLock.lockInterruptibly();
                try {
                    x = dequeue();
                    c = count.getAndDecrement();
                    if (c > 1)
                        notEmpty.signal();
                } finally {
                    queueChangeLock.unlock();
                }


        } finally {
            takeLock.unlock();
        }
        if (c == capacity)
            signalNotFull();
        return x;
    }

    /**
     * Retrieves and removes the head of this queue, or null if this queue is empty.
     **/
    public E poll() {
        final AtomicInteger count = this.count;
        if (count.get() == 0)
            return null;
        E x = null;
        int c = -1;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            if (count.get() > 0) {

                final ReentrantLock queueChangeLock = this.queueChangeLock;
                queueChangeLock.lockInterruptibly();
                try {
                    x = dequeue();
                    c = count.getAndDecrement();
                    if (c > 1)
                        notEmpty.signal();
                } finally {
                    queueChangeLock.unlock();
                }

            }
        } catch (InterruptedException e) {
            return x;
        } finally {
            takeLock.unlock();
        }
        if (c == capacity)
            signalNotFull();
        return x;
    }

    /**
     */
    public int drainTo(Collection<? super E> c) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        boolean signalNotFull = false;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            int i = 0;
            try {
                for(E value : multiMap.values()) {
                    c.add(value);
                    ++i;
                }
                multiMap.clear();
                count.getAndSet(0);
                return i;
            } finally {
                // Restore invariants even if c.add() threw
                signalNotFull = (count.get() < capacity);
            }
        } finally {
            takeLock.unlock();
            if (signalNotFull)
                 signalNotFull();
        }
    }

}
