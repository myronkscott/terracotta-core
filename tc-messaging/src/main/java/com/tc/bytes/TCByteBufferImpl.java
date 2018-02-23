/*
 *
 *  The contents of this file are subject to the Terracotta Public License Version
 *  2.0 (the "License"); You may not use this file except in compliance with the
 *  License. You may obtain a copy of the License at
 *
 *  http://terracotta.org/legal/terracotta-public-license.
 *
 *  Software distributed under the License is distributed on an "AS IS" basis,
 *  WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
 *  the specific language governing rights and limitations under the License.
 *
 *  The Covered Software is Terracotta Core.
 *
 *  The Initial Developer of the Covered Software is
 *  Terracotta, Inc., a Software AG company
 *
 */
package com.tc.bytes;

import com.tc.util.State;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author teck A thin wrapper to a real java.nio.ByteBuffer instance
 */

// XXX: Should we wrap the native java.nio overflow, underflow and readOnly exceptions with the TC versions?
// This would make the TCByteBuffer interface consistent w.r.t. exceptions (whilst being blind to JDK13 vs JDK14)
public class TCByteBufferImpl implements TCByteBuffer, BufferPool {

  private static final State        INIT        = new State("INIT");
  private static final State        CHECKED_OUT = new State("CHECKED_OUT");
  private static final State        COMMITTED   = new State("COMMITTED");

  private final ByteBuffer          buffer;
  private final TCByteBuffer        root;
  private final LinkedBlockingQueue<TCByteBuffer> bufPool;
  private State                     state       = INIT;

  TCByteBufferImpl(int capacity, boolean direct, LinkedBlockingQueue<TCByteBuffer> poolQueue) {
    if (direct) {
      buffer = ByteBuffer.allocateDirect(capacity);
    } else {
      buffer = ByteBuffer.allocate(capacity);
    }
    bufPool = poolQueue;
    root = this;
  }

  private TCByteBufferImpl(ByteBuffer buf) {
    buffer = buf;
    bufPool = null;
    this.root = null;
  }

  private TCByteBufferImpl(ByteBuffer buf, TCByteBuffer root) {
    buffer = buf;
    bufPool = null;
    this.root = root;
  }

  static TCByteBuffer wrap(byte[] data) {
    return new TCByteBufferImpl(ByteBuffer.wrap(data));
  }

  protected ByteBuffer getBuffer() {
    return buffer;
  }

  @Override
  public TCByteBuffer clear() {
    buffer.clear();
    return this;
  }

  @Override
  public int capacity() {
    return buffer.capacity();
  }

  @Override
  public int position() {
    return buffer.position();
  }

  @Override
  public TCByteBuffer flip() {
    buffer.flip();
    return this;
  }

  @Override
  public boolean hasRemaining() {
    return buffer.hasRemaining();
  }

  @Override
  public int limit() {
    return buffer.limit();
  }

  @Override
  public TCByteBuffer limit(int newLimit) {
    buffer.limit(newLimit);
    return this;
  }

  @Override
  public TCByteBuffer position(int newPosition) {
    buffer.position(newPosition);
    return this;
  }

  @Override
  public int remaining() {
    return buffer.remaining();
  }

  @Override
  public com.tc.bytes.TCByteBuffer rewind() {
    buffer.rewind();
    return this;
  }

  @Override
  public ByteBuffer getNioBuffer() {
    return buffer;
  }

  @Override
  public boolean isDirect() {
    return buffer.isDirect();
  }

  @Override
  public byte[] array() {
    return buffer.array();
  }

  @Override
  public byte get() {
    return buffer.get();
  }

  @Override
  public boolean getBoolean() {
    // XXX: Um-- why isn't there a getBoolean in ByteBuffer?
    return buffer.get() > 0;
  }

  @Override
  public boolean getBoolean(int index) {
    return buffer.get(index) > 0;
  }

  @Override
  public char getChar() {
    return buffer.getChar();
  }

  @Override
  public char getChar(int index) {
    return buffer.getChar(index);
  }

  @Override
  public double getDouble() {
    return buffer.getDouble();
  }

  @Override
  public double getDouble(int index) {
    return buffer.getDouble(index);
  }

  @Override
  public float getFloat() {
    return buffer.getFloat();
  }

  @Override
  public float getFloat(int index) {
    return buffer.getFloat(index);
  }

  @Override
  public int getInt() {
    return buffer.getInt();
  }

  @Override
  public int getInt(int index) {
    return buffer.getInt(index);
  }

  @Override
  public long getLong() {
    return buffer.getLong();
  }

  @Override
  public long getLong(int index) {
    return buffer.getLong(index);
  }

  @Override
  public short getShort() {
    return buffer.getShort();
  }

  @Override
  public short getShort(int index) {
    return buffer.getShort(index);
  }

  @Override
  public TCByteBuffer get(byte[] dst) {
    buffer.get(dst);
    return this;
  }

  @Override
  public TCByteBuffer get(byte[] dst, int offset, int length) {
    buffer.get(dst, offset, length);
    return this;
  }

  @Override
  public byte get(int index) {
    return buffer.get(index);
  }

  @Override
  public TCByteBuffer put(byte b) {
    buffer.put(b);
    return this;
  }

  @Override
  public TCByteBuffer put(byte[] src) {
    buffer.put(src);
    return this;
  }

  @Override
  public TCByteBuffer put(byte[] src, int offset, int length) {
    buffer.put(src, offset, length);
    return this;
  }

  @Override
  public TCByteBuffer put(int index, byte b) {
    buffer.put(index, b);
    return this;
  }

  @Override
  public TCByteBuffer putBoolean(boolean b) {
    // XXX: Why isn't there a putBoolean in ByteBuffer?
    buffer.put((b) ? (byte) 1 : (byte) 0);
    return this;
  }

  @Override
  public TCByteBuffer putBoolean(int index, boolean b) {
    buffer.put(index, (b) ? (byte) 1 : (byte) 0);
    return this;
  }

  @Override
  public TCByteBuffer putChar(char c) {
    buffer.putChar(c);
    return this;
  }

  @Override
  public TCByteBuffer putChar(int index, char c) {
    buffer.putChar(index, c);
    return this;
  }

  @Override
  public TCByteBuffer putDouble(double d) {
    buffer.putDouble(d);
    return this;
  }

  @Override
  public TCByteBuffer putDouble(int index, double d) {
    buffer.putDouble(index, d);
    return this;
  }

  @Override
  public TCByteBuffer putFloat(float f) {
    buffer.putFloat(f);
    return this;
  }

  @Override
  public TCByteBuffer putFloat(int index, float f) {
    buffer.putFloat(index, f);
    return this;
  }

  @Override
  public TCByteBuffer putInt(int i) {
    buffer.putInt(i);
    return this;
  }

  @Override
  public TCByteBuffer putInt(int index, int i) {
    buffer.putInt(index, i);
    return this;
  }

  @Override
  public TCByteBuffer putLong(long l) {
    buffer.putLong(l);
    return this;
  }

  @Override
  public TCByteBuffer putLong(int index, long l) {
    buffer.putLong(index, l);
    return this;
  }

  @Override
  public TCByteBuffer putShort(short s) {
    buffer.putShort(s);
    return this;
  }

  @Override
  public TCByteBuffer putShort(int index, short s) {
    buffer.putShort(index, s);
    return this;
  }

  @Override
  public TCByteBuffer duplicate() {
    return new TCByteBufferImpl(buffer.duplicate(), root);
  }

  @Override
  public TCByteBuffer put(TCByteBuffer src) {
    buffer.put(src.getNioBuffer());
    return this;
  }

  @Override
  public TCByteBuffer slice() {
    return new TCByteBufferImpl(buffer.slice(), root);
  }

  @Override
  public int arrayOffset() {
    return buffer.arrayOffset();
  }

  @Override
  public TCByteBuffer asReadOnlyBuffer() {
    return new TCByteBufferImpl(buffer.asReadOnlyBuffer(), root);
  }

  @Override
  public boolean isReadOnly() {
    return buffer.isReadOnly();
  }

  @Override
  public String toString() {
    return (buffer == null) ? "TCByteBufferJDK14(null buffer)" : "TCByteBufferJDK14@" + System.identityHashCode(this)
                                                                 + "(" + buffer.toString() + ")";
  }

  @Override
  public boolean hasArray() {
    return buffer.hasArray();
  }

  // Can be called only once on any of the views and the root is gone
  @Override
  public void recycle() {
    if (root != null) {
      TCByteBufferFactory.returnBuffer(root.reInit());
    }
  }

  @Override
  public TCByteBuffer reInit() {
    clear();
    return this;
  }

  @Override
  public final TCByteBuffer get(int index, byte[] dst) {
    return get(index, dst, 0, dst.length);
  }

  @Override
  public final TCByteBuffer get(int index, byte[] dst, int offset, int length) {
    final int origPosition = position();

    try {
      position(index);
      get(dst, offset, length);
    } finally {
      position(origPosition);
    }

    return this;
  }

  @Override
  public final TCByteBuffer put(int index, byte[] src) {
    return put(index, src, 0, src.length);
  }

  @Override
  public final TCByteBuffer put(int index, byte[] src, int offset, int length) {
    final int origPosition = position();

    try {
      position(index);
      put(src, offset, length);
    } finally {
      position(origPosition);
    }

    return this;
  }

  @Override
  public final TCByteBuffer putUint(long i) {
    if ((i > 0xFFFFFFFFL) || (i < 0L)) {
      // make code formatter sane
      throw new IllegalArgumentException("Unsigned integer value must be positive and <= (2^32)-1");
    }

    buffer.putInt((int)(i & 0xFFFFFFFF));

    return this;
  }

  @Override
  public final TCByteBuffer putUint(int index, long i) {
    final int origPosition = position();

    try {
      position(index);
      putUint(i);
    } finally {
      position(origPosition);
    }

    return this;
  }

  @Override
  public final TCByteBuffer putUshort(int s) {
    if ((s > 0x0000FFFF) || (s < 0)) { throw new IllegalArgumentException(
                                                                          "Unsigned integer value must be positive and <= (2^16)-1"); }

    buffer.putShort((short) (s & 0x0000FFFF));

    return this;
  }

  @Override
  public final TCByteBuffer putUshort(int index, int s) {
    final int origPosition = position();

    try {
      position(index);
      putUshort(s);
    } finally {
      position(origPosition);
    }

    return this;
  }

  @Override
  public final long getUint() {
    return buffer.getInt() & 0xFFFFFFFFL;
  }

  @Override
  public final long getUint(int index) {
    return buffer.getInt(index) & 0xFFFFFFFFL;
  }

  @Override
  public final int getUshort() {
    return buffer.getShort() & 0xFFFF;
  }

  @Override
  public final int getUshort(int index) {
    return buffer.getShort(index) & 0xFFFF;
  }

  @Override
  public final short getUbyte() {
    return (short) (buffer.get() & 0xFF);
  }

  @Override
  public final short getUbyte(int index) {
    return (short) (buffer.get(index) & 0xFF);
  }

  @Override
  public final TCByteBuffer putUbyte(int index, short value) {
    final int origPosition = position();

    try {
      position(index);
      putUbyte(value);
    } finally {
      position(origPosition);
    }

    return this;
  }

  @Override
  public final TCByteBuffer putUbyte(short value) {
    if ((value < 0) || (value > 0xFF)) { throw new IllegalArgumentException(
                                                                            "Unsigned byte value must in range 0-255 inclusive"); }
    buffer.put((byte) (value & 0xFF));
    return this;
  }

  @Override
  public void commit() {
    if (state == COMMITTED) { throw new AssertionError("Already commited"); }
    state = COMMITTED;
  }

  @Override
  public void checkedOut() {
    if (state == CHECKED_OUT) { throw new AssertionError("Already checked out"); }
    state = CHECKED_OUT;
  }

  @Override
  public BufferPool getBufferPool() {
    return this;
  }

  @Override
  public void offer(TCByteBuffer buf) throws InterruptedException {
    if (this.bufPool != null) {
      this.bufPool.offer(buf, 0, TimeUnit.MILLISECONDS);
    }
  }
}
