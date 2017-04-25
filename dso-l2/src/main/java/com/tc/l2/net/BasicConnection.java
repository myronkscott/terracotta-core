/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tc.l2.net;

import com.tc.net.TCSocketAddress;
import com.tc.net.core.TCConnection;
import com.tc.net.core.event.TCConnectionEvent;
import com.tc.net.core.event.TCConnectionEventListener;
import com.tc.net.protocol.TCNetworkMessage;
import com.tc.net.protocol.transport.WireProtocolHeader;
import com.tc.net.protocol.transport.WireProtocolMessage;
import com.tc.net.protocol.transport.WireProtocolMessageImpl;
import com.tc.util.Assert;
import com.tc.util.TCTimeoutException;
import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 * @author mscott
 */
public class BasicConnection implements TCConnection {
  
  private final long connect = System.currentTimeMillis();
  private volatile long last = System.currentTimeMillis();
  private volatile long received = System.currentTimeMillis();
  
  private final Function<TCConnection, Socket> closeRunnable;
  private final Consumer<WireProtocolMessage> write;
  private Socket src;
  private boolean established = false;
  private boolean connected = true;
  private List<TCConnectionEventListener> listeners = new LinkedList<>();

  public BasicConnection(Socket src, Consumer<WireProtocolMessage> write, Function<TCConnection, Socket> close) {
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
    return System.currentTimeMillis() - last;
  }

  @Override
  public long getIdleReceiveTime() {
    return System.currentTimeMillis() - received;
  }
  
  void marckReceived() {
    received = System.currentTimeMillis();
  }

  @Override
  public synchronized void addListener(TCConnectionEventListener listener) {
    listeners.add(listener);
  }

  @Override
  public synchronized void removeListener(TCConnectionEventListener listener) {
    listeners.remove(listener);
  }

  @Override
  public void asynchClose() {

  }

  @Override
  public synchronized Socket detach() throws IOException {
    try {
      this.established = false;
      return this.closeRunnable.apply(this);
    } catch (Exception e) {
      return null;
    } finally {
      this.established = false;
      this.connected = false;
    }
  }

  @Override
  public boolean close(long timeout) {
    try {
      detach().close();
      return true;
    } catch (IOException ioe) {
      return false;
    } finally {
      fireClosed();
    }
  }
  
  private void fireClosed() {
    TCConnectionEvent event = new TCConnectionEvent(this);
    listeners.forEach(l->l.closeEvent(event));
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
  public synchronized boolean isConnected() {
    return this.connected;
  }

  @Override
  public synchronized boolean isClosed() {
    return !this.connected;
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
  public synchronized void setTransportEstablished() {
    established = true;
  }

  @Override
  public synchronized boolean isTransportEstablished() {
    return established;
  }

  @Override
  public boolean isClosePending() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void putMessage(TCNetworkMessage message) {
    last = System.currentTimeMillis();
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
