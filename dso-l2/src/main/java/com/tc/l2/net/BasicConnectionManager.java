/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tc.l2.net;

import com.tc.net.TCSocketAddress;
import com.tc.net.core.TCComm;
import com.tc.net.core.TCConnection;
import com.tc.net.core.TCConnectionManager;
import com.tc.net.core.TCListener;
import com.tc.net.protocol.ProtocolAdaptorFactory;
import com.tc.net.protocol.TCProtocolAdaptor;
import java.io.IOException;
import javax.net.ServerSocketFactory;

/**
 *
 * @author mscott
 */
public class BasicConnectionManager implements TCConnectionManager {

  @Override
  public TCConnection createConnection(TCProtocolAdaptor adaptor) {
    return new BasicConnection(null, (t)->{}, (conn)->null);
  }

  @Override
  public TCListener createListener(TCSocketAddress addr, ProtocolAdaptorFactory factory) throws IOException {
    return new BasicListenerImpl(factory, ServerSocketFactory.getDefault(), addr.getAddress(), addr.getPort());
  }

  @Override
  public TCListener createListener(TCSocketAddress addr, ProtocolAdaptorFactory factory, int backlog, boolean reuseAddr) throws IOException {
    return new BasicListenerImpl(factory, ServerSocketFactory.getDefault(), addr.getAddress(), addr.getPort());
  }

  @Override
  public void asynchCloseAllConnections() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void closeAllConnections(long timeout) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void closeAllListeners() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public TCConnection[] getAllConnections() {
    return new TCConnection[0];
  }

  @Override
  public TCConnection[] getAllActiveConnections() {
    return new TCConnection[0];
  }

  @Override
  public TCListener[] getAllListeners() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public TCComm getTcComm() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
}
