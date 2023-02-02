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
package com.tc.net.protocol.transport;

/**
 * This class models the payload portion of a TC wire protocol message. Not Thread Safe!!
 * 
 * <pre>
 *        0                   1                   2                   3
 *        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2
 *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *        |                       WireProtocolHeader                      | 
 *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *        |                       Message 1 Length                        |
 *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *        |       Message 1 Protocol      |       Message 1 data        ...
 *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *        |                       Message 2 Length                        |
 *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *        |       Message 2 Protocol      |       Message 2 data        ...
 *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *        .                               .                               .   
 *        .                               .                               . 
 *        .                               .                               .
 *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *        |                       Message n Length                        |
 *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *        |       Message n Protocol      |       Message n data        ...        
 *        +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * 
 * </pre>
 */

import com.tc.bytes.TCByteBuffer;
import com.tc.bytes.TCByteBufferFactory;
import com.tc.bytes.TCByteBufferReference;
import com.tc.io.TCByteBufferInputStream;
import com.tc.net.core.TCConnection;
import com.tc.net.protocol.TCNetworkMessage;
import com.tc.net.protocol.TCNetworkMessageImpl;
import com.tc.net.protocol.TCProtocolException;
import com.tc.net.protocol.tcm.TCActionNetworkMessage;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class WireProtocolGroupMessageImpl extends TCNetworkMessageImpl implements WireProtocolGroupMessage {

  private final TCConnection                sourceConnection;
  private final List<TCActionNetworkMessage> messagePayloads;
  private final Optional<TCByteBufferReference.Ref> messageSource;

  public static WireProtocolGroupMessageImpl wrapMessages(List<TCActionNetworkMessage> msgPayloads,
                                                          TCConnection source) {
    WireProtocolHeader header = new WireProtocolHeader();
    header.setProtocol(WireProtocolHeader.PROTOCOL_MSGGROUP);

    return new WireProtocolGroupMessageImpl(source, header, msgPayloads);
  }

  // used by the reader
  protected WireProtocolGroupMessageImpl(TCConnection source, WireProtocolHeader header,
                                         TCByteBufferReference messagePayloadByteBuffers) {
    super(header);
    TCByteBufferReference.Ref buffers = messagePayloadByteBuffers.reference();
    setPayload(buffers.asArray());
    addCompleteCallback(buffers::close);
    this.messageSource = Optional.of(buffers);
    this.sourceConnection = source;
    messagePayloads = null;
  }
  // used by the writer
  protected WireProtocolGroupMessageImpl(TCConnection source, WireProtocolHeader header,
                                         List<TCActionNetworkMessage> messagePayloads) {
    super(header);
    this.sourceConnection = source;
    this.messagePayloads = messagePayloads;
    this.messageSource = Optional.empty();
  }

  @Override
  public boolean prepareToSend() {
      setPayload(generatePayload());
      getWireProtocolHeader().setMessageCount(messagePayloads.size());
      getWireProtocolHeader().finalizeHeader(getTotalLength());
      return getWireProtocolHeader().getMessageCount() > 0;
  }
  
  private TCByteBuffer[] generatePayload() {
    List<TCByteBuffer> msgs = new ArrayList<>(messagePayloads.size() * 2);
    Iterator<TCActionNetworkMessage> msgI = messagePayloads.iterator();
    while (msgI.hasNext()) {
      TCActionNetworkMessage msg = msgI.next();
      if (msg.commit()) {
        TCByteBuffer tcb = TCByteBufferFactory.getInstance((Integer.SIZE + Short.SIZE) / 8);
        tcb.putInt(msg.getTotalLength());
        tcb.putShort(WireProtocolHeader.getProtocolForMessageClass(msg));
        tcb.flip();

        msgs.add(tcb);
        // referring to the original payload buffers
        msgs.addAll(Arrays.asList(msg.getEntireMessageData()));
      } else {
        msg.complete();
        msgI.remove();
      }
    }
    return msgs.toArray(new TCByteBuffer[msgs.size()]);
  }
  
  private List<TCNetworkMessage> getMessagesFromByteBuffers() throws IOException {
    ArrayList<TCNetworkMessage> messages = new ArrayList<>();

    TCByteBufferReference.Ref src = messageSource.get();

    try (TCByteBufferInputStream msgs = new TCByteBufferInputStream(messageSource.get().asArray())) {
      for (int i = 0; i < getWireProtocolHeader().getMessageCount(); i++) {
        int msgLen = msgs.readInt();
        short msgProto = msgs.readShort();

        WireProtocolHeader hdr;
        hdr = (WireProtocolHeader) getWireProtocolHeader().clone();
        hdr.setTotalPacketLength(hdr.getHeaderByteLength() + msgLen);
        hdr.setProtocol(msgProto);
        hdr.setMessageCount(1);
        hdr.computeChecksum();
        WireProtocolMessage msg = new WireProtocolMessageImpl(this.sourceConnection, hdr, limit(src.duplicate(), msgLen));
        Iterator<TCByteBuffer> test = limit(src.duplicate(), msgLen).iterator();
        msgs.skip(msgLen);
//        WireProtocolMessage msg = new WireProtocolMessageImpl(this.sourceConnection, hdr, new TCByteBufferReference(Arrays.asList(new TCByteBuffer[] {msgs.read(msgLen)}), new LinkedList<>()));
        messages.add(msg);
      }
    }

    return messages;
  }
  
  private TCByteBufferReference.Ref limit(TCByteBufferReference.Ref buffers, int limit) {
    Iterator<TCByteBuffer> bufs = buffers.iterator();
    int available = 0;
    while (bufs.hasNext()) {
      TCByteBuffer buf = bufs.next();
      int remain = buf.remaining();
      if (!buf.hasRemaining()) {
        bufs.remove();
      } else if (available + remain >= limit) {
        buf.limit(buf.position() + limit - available);
        bufs.forEachRemaining(r->r.position(r.limit()));
      } else {
        available += remain;
      }
    }
    return buffers;
  }

  @Override
  public Iterator<TCNetworkMessage> getMessageIterator() throws TCProtocolException {
    try {
      return getMessagesFromByteBuffers().iterator();
    } catch (IOException ioe) {
      throw new TCProtocolException(ioe);
    }
  }

  @Override
  public int getTotalMessageCount() {
    return ((WireProtocolHeader) getHeader()).getMessageCount();
  }

  @Override
  public TCConnection getSource() {
    return this.sourceConnection;
  }

  @Override
  public WireProtocolHeader getWireProtocolHeader() {
    return ((WireProtocolHeader) getHeader());
  }

  @Override
  public short getMessageProtocol() {
    return ((WireProtocolHeader) getHeader()).getProtocol();
  }

  @Override
  public void complete() {
    if (this.messagePayloads != null) {
      this.messagePayloads.iterator().forEachRemaining(TCNetworkMessage::complete);
    }
    super.complete();
  }

  @Override
  public boolean isValid() {
    return !this.messagePayloads.stream().allMatch(TCActionNetworkMessage::isCancelled);
  }
}
