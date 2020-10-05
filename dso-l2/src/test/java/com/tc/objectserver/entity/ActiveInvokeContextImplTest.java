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
package com.tc.objectserver.entity;

import com.tc.net.ClientID;
import com.tc.object.ClientInstanceID;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import org.terracotta.entity.ActiveInvokeChannel;
import org.terracotta.entity.EntityResponse;

public class ActiveInvokeContextImplTest {

  @Test
  public void testValid() {
    ActiveInvokeContextImpl ctx = new ActiveInvokeContextImpl(
      new ClientDescriptorImpl(new ClientID(1), new ClientInstanceID(2)),
      1,
      1,
      2);
    Assert.assertThat(ctx.isValidClientInformation(), is(true));
  }

  @Test
  public void testInvalid() {
    ActiveInvokeContextImpl ctx = new ActiveInvokeContextImpl(new ClientDescriptorImpl(), 1, 1, 2);
    Assert.assertThat(ctx.isValidClientInformation(), is(false));
    ctx = new ActiveInvokeContextImpl(new ClientDescriptorImpl(), 1, -1, -1);
    Assert.assertThat(ctx.isValidClientInformation(), is(false));
    ctx = new ActiveInvokeContextImpl(new ClientDescriptorImpl(new ClientID(1), new ClientInstanceID(2)), 1, -1, 2);
    Assert.assertThat(ctx.isValidClientInformation(), is(true));
    ctx = new ActiveInvokeContextImpl(new ClientDescriptorImpl(new ClientID(1), new ClientInstanceID(2)), 1, 1, -1);
    Assert.assertThat(ctx.isValidClientInformation(), is(false));
  }

  @Test
  public void testSendOnClosed() {
    Runnable open = mock(Runnable.class);
    Consumer response = mock(Consumer.class);
    Consumer exception = mock(Consumer.class);
    Runnable close = mock(Runnable.class);
    
    ActiveInvokeContextImpl ctx = new ActiveInvokeContextImpl(new ClientDescriptorImpl(), 1, 1, 2, 
        open, response, exception, close);
    ActiveInvokeChannel chan = ctx.openInvokeChannel();
    verify(open).run();
    chan.sendResponse(mock(EntityResponse.class));
    verify(response).accept(any(EntityResponse.class));
    chan.close();
    verify(close).run();
    try {
      chan.sendResponse(mock(EntityResponse.class));
      Assert.fail();
    } catch (IllegalStateException state) {
      // expected
    }
  }
}