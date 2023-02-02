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
package com.tc.net.protocol.tcm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tc.bytes.TCByteBuffer;
import com.tc.io.TCByteBufferInputStream;
import com.tc.net.protocol.TCNetworkMessage;
import com.tc.util.Assert;
import java.util.Arrays;

/**
 * A class that knows how to parse TCMessages out of raw bytes
 */
class TCMessageParser {
   private static final Logger logger = LoggerFactory.getLogger(TCMessageParser.class);
  private final TCMessageFactory factory;

  TCMessageParser(TCMessageFactory factory) {
    this.factory = factory;
  }

  TCAction parseMessage(MessageChannel source, TCNetworkMessage msg) {
    TCByteBuffer[] data = msg.getPayload();
    TCMessageHeader hdr = new TCMessageHeaderImpl(data[0].slice().limit(TCMessageHeader.HEADER_LENGTH));
    final int headerLength = hdr.getHeaderByteLength();

    if (headerLength != TCMessageHeader.HEADER_LENGTH) {
      logger.error("Invalid header length ! length = " + headerLength);
      logger.error("error header = " + hdr);
      logger.error(" buffer data is " + toString(data));
      throw new RuntimeException("Invalid header length: " + headerLength);
    }

    data[0].position(headerLength + data[0].position());
    final TCByteBuffer msgData[] = Arrays.asList(data).stream().filter(TCByteBuffer::hasRemaining).map(TCByteBuffer::slice).toArray(TCByteBuffer[]::new);

    final int msgType = hdr.getMessageType();
    final TCMessageType type = TCMessageType.getInstance(hdr.getMessageType());

    if (type == null) {
      throw new RuntimeException("Can't find message type for type: " + msgType);
    }
    
    TCAction converted = factory.createMessage(source, type, hdr, new TCByteBufferInputStream(msgData, msg.stealCompleteAction()));
    
    return converted;
  }

  private String toString(TCByteBuffer[] data) {
    if(data == null || data.length == 0) { return "null or size 0"; }
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < data.length; i++) {
      sb.append(data[i]);
      sb.append(" { ");
      if (data[i].hasArray()) {
        byte b[] = data[i].array();
        for (int j = 0; j < b.length; j++) {
          sb.append(b[j]).append(" ");
        }
      }
      sb.append(" } ");
    }
    return sb.toString();
  }
}
