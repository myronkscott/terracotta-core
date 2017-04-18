/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tc.l2.net;

import com.tc.bytes.TCByteBuffer;
import com.tc.net.TCSocketAddress;
import com.tc.net.core.TCListener;
import com.tc.net.core.event.TCListenerEventListener;
import com.tc.net.protocol.ProtocolAdaptorFactory;
import com.tc.net.protocol.TCProtocolAdaptor;
import com.tc.net.protocol.TCProtocolException;
import com.tc.net.protocol.transport.WireProtocolMessage;
import com.tc.util.Assert;
import com.tc.util.TCTimeoutException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import javax.net.ServerSocketFactory;

/**
 *
*/
public class BasicListenerImpl implements TCListener, Runnable {
  
  private final InetAddress bind;
  private final int port;
  private final ServerSocket server;
  private final Thread spinner;
  private boolean running = true;
  private boolean finished = false;
  private final ProtocolAdaptorFactory factory;
  private final ExecutorService readerExec = Executors.newCachedThreadPool();
  private final Set<BasicConnection> connections = Collections.synchronizedSet(new LinkedHashSet<>());

  public BasicListenerImpl(ProtocolAdaptorFactory protocol, ServerSocketFactory factory, InetAddress bind, int port) throws IOException {
    this.bind = bind;
    this.port = port;
    this.server = factory.createServerSocket();
    this.server.setReuseAddress(true);
    SocketAddress address = new InetSocketAddress(bind, port);
    this.server.bind(address);
    this.spinner = new Thread(this);
    this.spinner.setDaemon(true);
    this.factory = protocol;
    this.start();
  }
  
  private void start() {
    this.spinner.start();
  }
  
  public void run() {
    try {
      while (running) {
        Socket socket = this.server.accept();
        InputStream is = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        TCProtocolAdaptor adaptor = this.factory.getInstance();
        BasicConnection connection = new BasicConnection(adaptor, socket, (msg)->write(out,msg), (conn)->{connections.remove(conn);return socket;});
        connections.add(connection);
        readerExec.submit(() -> {
          try {
            while (!socket.isInputShutdown()) {
              int amount = 0;
              TCByteBuffer[] buffers = adaptor.getReadBuffers();
              for (TCByteBuffer buf : buffers) {
                byte[] read = buf.array();
                while (buf.hasRemaining()) {
                  int count = is.read(read, buf.arrayOffset() + buf.position(), buf.remaining());
                  if (count >= 0) {
                    buf.put(read, buf.arrayOffset() + buf.position(), count);
                    amount += count;
                  } else {
                    break;
                  }
                }
              }
              adaptor.addReadData(connection, buffers, amount);
            }            
          } catch (IOException ioe) {
            ioe.printStackTrace();
            // TODO:  figure it out
          } catch (TCProtocolException proto) {
            proto.printStackTrace();
          } catch (Throwable t) {
            t.printStackTrace();
          }
        });
      }
    } catch (IOException ioe) {
      
    }
    synchronized (this) {
      finished = true;
      this.notifyAll();
    }
  }
  
  public void write(OutputStream out, WireProtocolMessage message) {
    Semaphore latch = new Semaphore(0);
    this.readerExec.submit(()->{
      try {
        TCByteBuffer[] data = message.getEntireMessageData();
        for (TCByteBuffer buf : data) {
          out.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        }
        latch.release();
        out.flush();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      } catch (Throwable t) {
        t.printStackTrace();
      }
    });
    latch.acquireUninterruptibly();
  } 

  @Override
  public void stop() {
    try {
      this.stop(0);
    } catch (TCTimeoutException te) {
      throw Assert.failure(te);
    }
  }

  @Override
  public synchronized void stop(long timeout) throws TCTimeoutException {
    this.running = false;
    this.spinner.interrupt();
    try {
      this.server.close();
      while (!this.finished) {
        this.wait(timeout);
      }
      if (!this.finished) {
        throw new TCTimeoutException(timeout);
      }
    } catch (InterruptedException ie) {
      // don't care about waiting
    } catch (IOException ioe) {
      
    } catch (TCTimeoutException to) {
      
    }
  }

  @Override
  public int getBindPort() {
    return port;
  }

  @Override
  public InetAddress getBindAddress() {
    return bind;
  }

  @Override
  public TCSocketAddress getBindSocketAddress() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void addEventListener(TCListenerEventListener lsnr) {

  }

  @Override
  public void removeEventListener(TCListenerEventListener lsnr) {

  }

  @Override
  public boolean isStopped() {
    return false;
  }
  
}
