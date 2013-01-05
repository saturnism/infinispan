/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * 
 * This strategy will always go to the first server in the server list.
 * This is useful, for example, when there is a need to setup an active-passive cluster, where
 * all requests must be directed to active node.  If the active node failed, that node will be
 * removed from HotRod client's server list, and the passive node (next in line) will be returned.
 * 
 * Note, the order of the servers specified in the server list is not guaranteed.  I.e., if you
 * specify the server list to be <i>"primary-server;secondary-server"</i>, the first server is not
 * guaranteed to be <i>primary-server</i>.  This is because, ultimately, the server list is stored
 * in a <code>HashSet</code>.
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 */
@ThreadSafe
public class AlwaysFirstBalancingStrategy implements RequestBalancingStrategy {

   private static final Log log = LogFactory.getLog(AlwaysFirstBalancingStrategy.class);

   private SocketAddress[] servers;

   @Override
   public void setServers(Collection<SocketAddress> servers) {
      this.servers = servers.toArray(new InetSocketAddress[servers.size()]);
      if (log.isTraceEnabled()) {
         log.tracef("New server list is: " + Arrays.toString(this.servers));
      }
   }

   /**
    * Multiple threads might call this method at the same time.
    */
   @Override
   public SocketAddress nextServer() {
      return servers[0];
   }
}
