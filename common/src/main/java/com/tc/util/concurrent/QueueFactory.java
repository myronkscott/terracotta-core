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
package com.tc.util.concurrent;

import java.util.concurrent.BlockingQueue;
import com.tc.async.impl.Event;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.SynchronousQueue;

public class QueueFactory {

  public <E> BlockingQueue<Event> createInstance(Class<E> type) {
    return new LinkedTransferQueue<>();
  }

  public <E> BlockingQueue<Event> createInstance(Class<E> type, int capacity) {
    if (capacity == 0) {
      return new SynchronousQueue<>();
    } else if (capacity < 0 || capacity > 8192) {
      return new LinkedTransferQueue<>();
    } else {
      return new ArrayBlockingQueue<>(capacity);
    }
  }
}
