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
package com.tc.net.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tc.bytes.TCByteBuffer;
import com.tc.exception.TCInternalError;
import com.tc.util.Assert;
import com.tc.util.HexDump;
import com.tc.util.concurrent.SetOnceFlag;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for network messages
 * 
 * @author teck
 */
public class TCNetworkMessageImpl implements TCNetworkMessage {
  protected static final Logger logger = LoggerFactory.getLogger(TCNetworkMessage.class);
  private static final int        MESSAGE_DUMP_MAXBYTES = 4 * 1024;

  protected TCNetworkMessageImpl(TCNetworkHeader header) {
    Assert.eval(header != null);
    this.header = header;
  }

  @Override
  public final int getDataLength() {
    checkSealed();
    return dataLength;
  }

  @Override
  public final int getHeaderLength() {
    checkSealed();
    return headerLength;
  }

  @Override
  public final int getTotalLength() {
    checkSealed();
    return totalLength;
  }

  @Override
  public final TCNetworkHeader getHeader() {
    return header;
  }

  @Override
  public final TCByteBuffer[] getPayload() {
    Assert.eval(payloadData != null);

    return payloadData;
  }

  protected final void setPayload(TCByteBuffer[] newPayload) {
    // this array should have already been set in seal()
    Assert.eval(payloadData == null);
    if (newPayload == null) {
      payloadData = EMPTY_BUFFER_ARRAY;
    } else {
      payloadData = newPayload;
    }
    seal();
  }

  @Override
  public final TCByteBuffer[] getEntireMessageData() {
    // this array should have already been set in seal()
    Assert.eval(entireMessageData != null);

    return entireMessageData;
  }

  @Override
  public final String toString() {
    try {
      return toString0();
    } catch (Exception e) {
      logger.warn("Exception in toString()", e);
      return "EXCEPTION in toString(): " + e.getMessage();
    }
  }

  protected final String toString0() {
    StringBuilder buf = new StringBuilder();
    buf.append("Message Class: ").append(getClass().getName()).append("\n");
    buf.append("Sealed: ").append((payloadData != null)).append(", ");
    buf.append("Header Length: ").append(getHeaderLength()).append(", ");
    buf.append("Data Length: ").append(getDataLength()).append(", ");
    buf.append("Total Length: ").append(getTotalLength()).append("\n");

    String extraMsgInfo = describeMessage();
    if (extraMsgInfo != null) {
      buf.append(extraMsgInfo).append("\n");
    }
    String payload = describePayload();
    if (payload != null) {
      buf.append(payload);
    }

    return buf.toString();
  }

  // override this method to add more information about your message
  protected String describeMessage() {
    return null;
  }
  
  protected String describePayload() {
    return null;
  }

  // override this method to add more description to your payload data
  protected String messageBytes() {
    StringBuilder buf = new StringBuilder();
    int totalBytesDumped = 0;
    if ((payloadData != null) && (payloadData.length != 0)) {
      for (int i = 0; i < payloadData.length; i++) {
        buf.append("Buffer ").append(i).append(": ");
        if (payloadData[i] != null) {

          buf.append(payloadData[i].toString());
          buf.append("\n");

          if (totalBytesDumped < MESSAGE_DUMP_MAXBYTES) {
            int bytesFullBuf = payloadData[i].limit();
            int bytesToDump = (((totalBytesDumped + bytesFullBuf) < MESSAGE_DUMP_MAXBYTES) ? bytesFullBuf
                : (MESSAGE_DUMP_MAXBYTES - totalBytesDumped));

            buf.append(HexDump.dump(payloadData[i].array(), payloadData[i].arrayOffset(), bytesToDump));
            totalBytesDumped += bytesToDump;
          }
        } else {
          buf.append("null");
        }
      }
    } else {
      buf.append("No payload buffers present");
    }

    return buf.toString();
  }

  protected String dump() {
    StringBuilder toRet = new StringBuilder(toString());
    toRet.append("\n\n");
    if (entireMessageData != null) {
      for (int i = 0; i < entireMessageData.length; i++) {
        toRet.append('[').append(i).append(']').append('=').append(entireMessageData[i].toString());
        toRet.append(" =  { ");
        byte ba[] = entireMessageData[i].array();
        for (byte element : ba) {
          toRet.append(Byte.toString(element)).append(' ');
        }
        toRet.append(" }  \n\n");
      }
    }
    return toRet.toString();
  }

  private final void seal() {
    final int size = 1 + payloadData.length;
    entireMessageData = new TCByteBuffer[size];
    entireMessageData[0] = header.getDataBuffer();
    System.arraycopy(payloadData, 0, entireMessageData, 1, payloadData.length);

    long dataLen = 0;
    for (int i = 1; i < entireMessageData.length; i++) {
      dataLen += entireMessageData[i].remaining();
    }

    if (dataLen > Integer.MAX_VALUE) { throw new TCInternalError("Message too big"); }

    this.dataLength = (int) dataLen;
    this.headerLength = header.getHeaderByteLength();
    this.totalLength = this.headerLength + this.dataLength;
  }

  @Override
  public void complete() {
    fireCallbacks();
  }

  private void fireCallbacks() {
    if (callbackFired.attemptSet()) {
      complete.complete(null);
    }
  }

  @Override
  public void addCompleteCallback(Runnable r) {
    complete.thenRun(r);
  }

  @Override
  public Runnable stealCompleteAction() {
    callbackFired.set(); // will throw if already set which is an assertion
    return ()->complete.complete(null);
  }
  
  private void checkSealed() {
    // this check is not thread safe
    if (payloadData == null) throw new IllegalStateException("Message is not sealed");
  }
  
  private final SetOnceFlag           callbackFired  = new SetOnceFlag();
  private static final TCByteBuffer[] EMPTY_BUFFER_ARRAY = {};
  private final TCNetworkHeader       header;
  private TCByteBuffer[]              payloadData;
  private TCByteBuffer[]              entireMessageData;
  private int                         totalLength;
  private int                         dataLength;
  private int                         headerLength;
  private final CompletableFuture<Void>       complete = new CompletableFuture<>();
}
