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
package com.tc.net.core;

import com.tc.bytes.TCByteBuffer;
import com.tc.bytes.TCByteBufferFactory;
import com.tc.bytes.TCReference;
import com.tc.bytes.TCReferenceSupport;
import static com.tc.net.core.SocketEndpoint.ResultType.EOF;
import static com.tc.net.core.SocketEndpoint.ResultType.OVERFLOW;
import static com.tc.net.core.SocketEndpoint.ResultType.SUCCESS;
import static com.tc.net.core.SocketEndpoint.ResultType.UNDERFLOW;
import static com.tc.net.core.SocketEndpoint.ResultType.ZERO;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 */
public class TCSocketReader implements AutoCloseable {
  private final Function<Integer, TCByteBuffer> allocator;
  private final Consumer<TCByteBuffer> returns;
  private TCByteBuffer raw;
  private TCReference current;
  private int readTo = 0;

  public TCSocketReader(Function<Integer, TCByteBuffer> allocator, Consumer<TCByteBuffer> returns) {
    this.allocator = allocator;
    this.returns = returns;
  }
  
  public TCReference readFromSocket(SocketEndpoint endpoint, int len) throws IOException {
    if (raw == null || !raw.hasRemaining()) {
      raw = allocator.apply(len);
      replaceCurrent(raw, createCompleteReference(raw), 0);
    }
    if (raw.position() - readTo >= len) {
    //  bytes are already off the network, return a slice for reading
      current.stream().findFirst().get().clear().position(readTo).limit(readTo + len);
      TCReference ref = current.duplicate();
      readTo += len;
      return ref;
    } else {
      // need to fetch bytes from the network
      LinkedList<TCByteBuffer> newBufs = new LinkedList<>();
      int capacity = raw.capacity() - readTo;
      // make sure there is enough capacity
      while (capacity < len) {
        TCByteBuffer next = allocator.apply(len - capacity);
        newBufs.add(next);
        capacity += next.capacity();
      }
      // but the current buffer at the head
      newBufs.addFirst(raw);
      int received = raw.position() - readTo;
      // read bytes from the network
      while (received < len) {
        try {
          received += doRead(endpoint, newBufs);
        } catch (NoBytesAvailable no) {
          newBufs.removeFirst();
          newBufs.forEach(returns::accept);
          return null;
        }
      }
      //  create the correct references
      // remove the current from the list of new buffers
      newBufs.removeFirst();
      if (newBufs.isEmpty()) {
      // if no new buffers, return a slice of bytes needed
        current.stream().findFirst().get().position(readTo).limit(readTo + len);
        readTo += len;
        return current.duplicate();
      } else {
     // created new buffers
     // make sure the current limit matches the underlying incase a OVERFLOW occurred
        current.stream().findFirst().get().limit(raw.limit());
     // remove the last buffer, this will be the new current buffer
        TCByteBuffer last = newBufs.removeLast();
     // position the front buffer to the right spot
        int built = current.stream().findFirst().get().position(readTo).remaining();
     // flip the whole middle, these will only serve the current request
        for (TCByteBuffer b : newBufs) {
          b.flip();
          built += b.remaining();
        }
     // create what will be the new current buffer
        TCReference lastRef = createCompleteReference(last);
     // set the end of the buffer for slicing
        int lastLim = lastRef.stream().findFirst().get().clear().position(0).limit(len - built).limit();

        try (TCReference newRefs = TCReferenceSupport.createReference(newBufs, returns)) {
     // piece together all the buffers and return a reference
          return TCReferenceSupport.createAggregateReference(current, newRefs, lastRef);
        } finally {
     // set the last buffer to the current, it may have bytes for the next message
          replaceCurrent(last, lastRef, lastLim);
        }
      }
    }
  }
  
  private void replaceCurrent(TCByteBuffer raw, TCReference ref, int pos) {
    if (this.current != null) {
      this.current.close();
    }
    this.raw = raw;
    this.current = ref;
    this.readTo = pos;
  }
  
  private TCReference createCompleteReference(TCByteBuffer buffer) {
    int pos = buffer.position();
    try {
      return TCReferenceSupport.createReference(returns, buffer.clear());
    } finally {
      buffer.position(pos);
    }
  }
  
  private static void returnByteBuffers(List<TCByteBuffer> dest, ByteBuffer[] raw) {
    for (int x=0;x<raw.length;x++) {
      dest.get(x).returnNioBuffer(raw[x]);
    }
  }
  
  private static ByteBuffer[] extractByteBuffers(List<TCByteBuffer> dest) {
    return dest.stream().map(TCByteBuffer::getNioBuffer).toArray(ByteBuffer[]::new);
  }
  
  private int doRead(SocketEndpoint endpoint, LinkedList<TCByteBuffer> dest) throws IOException {
    int remain = dest.stream().map(TCByteBuffer::remaining).reduce(Integer::sum).orElse(0);
    ByteBuffer[] nioBytes = extractByteBuffers(dest);
    try {
      switch (endpoint.readTo(nioBytes)) {
        case EOF:
          throw new EOFException();
        case OVERFLOW:
     // overflow, don't accept any more bytes in the current buffer
          dest.getLast().limit(dest.getLast().position());
     // add a buffer to capture bytes
          dest.add(allocator.apply(TCByteBufferFactory.getFixedBufferSize()));
          break;
        case UNDERFLOW:
          // unexpected
          throw new IOException();
        case SUCCESS:
          break;
        case ZERO:
          throw new NoBytesAvailable();
      }
    } finally {
      remain -= dest.stream().map(TCByteBuffer::remaining).reduce(Integer::sum).orElse(0);
      returnByteBuffers(dest, nioBytes);
    }
    return remain;
  }
  
  public void close() {
    if (current != null) {
      current.close();
    }
  }
  
  private static class NoBytesAvailable extends IOException {
    
  }
}
