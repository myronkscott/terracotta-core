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
package com.tc.async.impl;

import com.tc.async.api.Sink;
import java.util.function.Predicate;

/**
 *
 */
public class FilterSink<EC> implements Sink<EC> {
  private final Sink<EC> next;
  private final Predicate<EC> filter;
  
  public FilterSink(Sink<EC> next, Predicate<EC> filter) {
    this.next = next;
    this.filter = filter;
  }
  
  @Override
  public void addToSink(EC context) {
    if (filter.test(context)) {
      next.addToSink(context);
    }
  }
  
}
