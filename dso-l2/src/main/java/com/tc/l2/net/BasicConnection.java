/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tc.l2.net;

import com.tc.net.TCSocketAddress;
import com.tc.net.core.TCConnection;
import com.tc.net.core.event.TCConnectionEventListener;
import com.tc.net.protocol.TCNetworkMessage;
import com.tc.net.protocol.TCProtocolAdaptor;
import com.tc.net.protocol.transport.WireProtocolHeader;
import com.tc.net.protocol.transport.WireProtocolMessage;
import com.tc.net.protocol.transport.WireProtocolMessageImpl;
import com.tc.util.Assert;
import com.tc.util.TCTimeoutException;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 * @author mscott
 */
public class BasicConnection implements TCConnection {
  
  private final long connect = System.currentTimeMillis();
  private final Function<TCConnection, Socket> closeRunnable;
  private final Consumer<WireProtocolMessage> write;
  private final TCProtocolAdaptor protocol;
  private Socket src;
  private boolean established = false;

  public BasicConnection(TCProtocolAdaptor protocol, Socket src, Consumer<WireProtocolMessage> write, Function<TCConnection, Socket> close) {
    this.protocol = protocol;
    this.src = src;
    this.write = write;
    this.closeRunnable = close;
  }

  @Override
  public long getConnectTime() {
    return connect;
  }

  @Override
  public long getIdleTime() {
    return 0;
  }

  @Override
  public long getIdleReceiveTime() {
    return 0;
  }

  @Override
  public void addListener(TCConnectionEventListener listener) {

  }

  @Override
  public void removeListener(TCConnectionEventListener listener) {

  }

  @Override
  public void asynchClose() {

  }

  @Override
  public Socket detach() throws IOException {
    try {
      return this.closeRunnable.apply(this);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public boolean close(long timeout) {
    try {
      detach().close();
      return true;
    } catch (IOException ioe) {
      return false;
    }
  }

  @Override
  public void connect(TCSocketAddress addr, int timeout) throws IOException, TCTimeoutException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean asynchConnect(TCSocketAddress addr) throws IOException {
    src = new Socket(addr.getAddress(), addr.getPort());
    return src.isConnected();
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public TCSocketAddress getLocalAddress() {
    return new TCSocketAddress(this.src.getLocalAddress(), this.src.getLocalPort());
  }

  @Override
  public TCSocketAddress getRemoteAddress() {
    return new TCSocketAddress(this.src.getInetAddress(), this.src.getPort());
  }

  @Override
  public void addWeight(int addWeightBy) {

  }

  @Override
  public void setTransportEstablished() {
    established = true;
  }

  @Override
  public boolean isTransportEstablished() {
    return established;
  }

  @Override
  public boolean isClosePending() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void putMessage(TCNetworkMessage message) {
    if (message instanceof WireProtocolMessage) {
      this.write.accept(finalizeWireProtocolMessage((WireProtocolMessage)message, 1));
    } else {
      this.write.accept(buildWireProtocolMessage(message));
    }
  }
    
  private WireProtocolMessage buildWireProtocolMessage(TCNetworkMessage message) {
    Assert.eval(!(message instanceof WireProtocolMessage));
    final TCNetworkMessage payload = message;

    WireProtocolMessage wireMessage = WireProtocolMessageImpl.wrapMessage(message, this);
    Assert.eval(wireMessage.getSentCallback() == null);

    final Runnable callback = payload.getSentCallback();
    if (callback != null) {
      wireMessage.setSentCallback(callback);
    }
    return finalizeWireProtocolMessage(wireMessage, 1);
  }

  private WireProtocolMessage finalizeWireProtocolMessage(WireProtocolMessage message, int messageCount) {
    final WireProtocolHeader hdr = (WireProtocolHeader) message.getHeader();
    hdr.setSourceAddress(getLocalAddress().getAddressBytes());
    hdr.setSourcePort(getLocalAddress().getPort());
    hdr.setDestinationAddress(getRemoteAddress().getAddressBytes());
    hdr.setDestinationPort(getRemoteAddress().getPort());
    hdr.setMessageCount(messageCount);
    hdr.computeChecksum();
    return message;
  } 
}
