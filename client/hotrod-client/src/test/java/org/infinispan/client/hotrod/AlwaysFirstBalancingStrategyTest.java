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
package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.AlwaysFirstBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 * 
 */
@Test (groups = "unit", testName = "client.hotrod.AlwaysFirstBalancingStrategyTest")
public class AlwaysFirstBalancingStrategyTest {
   SocketAddress addr1 = new InetSocketAddress("localhost",1111);
   SocketAddress addr2 = new InetSocketAddress("localhost",2222);
   SocketAddress addr3 = new InetSocketAddress("localhost",3333);
   SocketAddress addr4 = new InetSocketAddress("localhost",4444);
   private List<SocketAddress> defaultServers;
   private AlwaysFirstBalancingStrategy strategy;

   @BeforeMethod
   public void setUp() {
      strategy = new AlwaysFirstBalancingStrategy();
      defaultServers = new ArrayList<SocketAddress>();
      defaultServers.add(addr1);
      defaultServers.add(addr2);
      defaultServers.add(addr3);
      strategy.setServers(defaultServers);
   }

   public void simpleTest() {
	  for (int i = 0; i < 9; i++)
		  assertEquals(addr1, strategy.nextServer());
   }

   public void testAddServer() {
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.add(addr4);
      strategy.setServers(newServers);
      for (int i = 0; i < 12; i++)
		  assertEquals(addr1, strategy.nextServer());
   }

   public void testRemoveServer() {
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.remove(addr3);
      strategy.setServers(newServers);
      for (int i = 0; i < 9; i++)
		  assertEquals(addr1, strategy.nextServer());
   }

   public void testRemoveServerAfterActivity() {
	  for (int i = 0; i < 3; i++)
         assertEquals(addr1, strategy.nextServer());
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.remove(addr1);
      strategy.setServers(newServers);
      // the next server index is reset to 0 because it would have been out of bounds
      for (int i = 0; i < 9; i++)
		  assertEquals(addr2, strategy.nextServer());
   }
   
   public void testAddServerAfterActivity() {
	   for (int i = 0; i < 3; i++)
	         assertEquals(addr1, strategy.nextServer());
      List<SocketAddress> newServers = new ArrayList<SocketAddress>(defaultServers);
      newServers.add(addr4);
      strategy.setServers(newServers);
      // the next server index is still valid, so it is not reset
      for (int i = 0; i < 3; i++)
          assertEquals(addr1, strategy.nextServer());
   }
}
