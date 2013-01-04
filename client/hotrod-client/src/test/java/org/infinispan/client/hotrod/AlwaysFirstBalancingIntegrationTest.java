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

import org.infinispan.Cache;
import org.infinispan.client.hotrod.impl.transport.tcp.AlwaysFirstBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.testng.AssertJUnit.assertEquals;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 * 
 */
@Test(testName = "client.hotrod.AlwaysFirstBalancingIntegrationTest", groups="functional")
public class AlwaysFirstBalancingIntegrationTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(AlwaysFirstBalancingIntegrationTest.class);

   Cache c1;
   Cache c2;
   Cache c3;
   Cache c4;

   HotRodServer hotRodServer1;
   HotRodServer hotRodServer2;

   HotRodServer hotRodServer3;
   HotRodServer hotRodServer4;

   RemoteCache<String, String> remoteCache;

   private RemoteCacheManager remoteCacheManager;

   @Override
   protected void assertSupportedConfig() {
      return;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
	   ConfigurationBuilder builder = getDefaultClusteredCacheConfig(
				CacheMode.REPL_SYNC, true);
      c1 = TestCacheManagerFactory.createLocalCacheManager(false).getCache();
      c2 = TestCacheManagerFactory.createLocalCacheManager(false).getCache();
      c3 = TestCacheManagerFactory.createLocalCacheManager(false).getCache();
      registerCacheManager(c1.getCacheManager(), c2.getCacheManager(), c3.getCacheManager());

      hotRodServer1 = TestHelper.startHotRodServer(c1.getCacheManager());
      hotRodServer2 = TestHelper.startHotRodServer(c2.getCacheManager());
      hotRodServer3 = TestHelper.startHotRodServer(c3.getCacheManager());

      log.trace("Server 1 port: " + hotRodServer1.getPort());
      log.trace("Server 2 port: " + hotRodServer2.getPort());
      log.trace("Server 3 port: " + hotRodServer3.getPort());
      String servers = TestHelper.getServersString(hotRodServer1, hotRodServer2, hotRodServer3);
      log.trace("Server list is: " + servers);
      Properties props = new Properties();
      props.put("infinispan.client.hotrod.server_list", servers);
      props.put("infinispan.client.hotrod.request_balancing_strategy", AlwaysFirstBalancingStrategy.class.getName());
      remoteCacheManager = new RemoteCacheManager(props);
      remoteCache = remoteCacheManager.getCache();
   }

   @AfterTest(alwaysRun = true)
   public void tearDown() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer1, hotRodServer2, hotRodServer3, hotRodServer4);
   }

   public void testAlwaysFirstBalancing() {
      remoteCache.put("k1", "v1");
      remoteCache.put("k2", "v2");
      remoteCache.put("k3", "v3");
      
      assertEquals(0, c1.size());
      assertEquals(0, c2.size());
      assertEquals(3, c3.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals("v3", remoteCache.get("k3"));

      remoteCache.put("k4", "v1");
      remoteCache.put("k5", "v2");
      remoteCache.put("k6", "v3");
      remoteCache.put("k7", "v1");
      remoteCache.put("k8", "v2");
      remoteCache.put("k9", "v3");

      assertEquals(0, c1.size());
      assertEquals(0, c2.size());
      assertEquals(9, c3.size());
   }
   
   @Test(dependsOnMethods = "testAlwaysFirstBalancing")
   public void testAddNewHotrodServer() {
      c4 = TestCacheManagerFactory.createLocalCacheManager(false).getCache();
      hotRodServer4 = TestHelper.startHotRodServer(c4.getCacheManager());
      registerCacheManager(c4.getCacheManager());

      List<SocketAddress> serverAddresses = new ArrayList<SocketAddress>();
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer1.getPort()));
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer2.getPort()));
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer3.getPort()));
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer4.getPort()));

      AlwaysFirstBalancingStrategy balancer = getBalancer();
      balancer.setServers(serverAddresses);

      remoteCache.put("k1", "v1");
      remoteCache.put("k2", "v2");
      remoteCache.put("k3", "v3");
      remoteCache.put("k4", "v4");

      assertEquals(4, c1.size());
      assertEquals(0, c2.size());
      assertEquals(0, c3.size());
      assertEquals(0, c4.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals("v3", remoteCache.get("k3"));
      assertEquals("v4", remoteCache.get("k4"));

      remoteCache.put("k5", "v2");
      remoteCache.put("k6", "v3");
      remoteCache.put("k7", "v1");
      remoteCache.put("k8", "v2");
      remoteCache.put("k9", "v3");
      remoteCache.put("k10", "v3");
      remoteCache.put("k11", "v3");
      remoteCache.put("k12", "v3");

      assertEquals(12, c1.size());
      assertEquals(0, c2.size());
      assertEquals(0, c3.size());
      assertEquals(0, c4.size());
   }

   @Test(dependsOnMethods = "testAddNewHotrodServer")
   public void testStopServer() {
      remoteCache.put("k1", "v1");
      remoteCache.put("k2", "v2");
      remoteCache.put("k3", "v3");
      remoteCache.put("k4", "v4");

      assertEquals(4, c1.size());
      assertEquals(0, c2.size());
      assertEquals(0, c3.size());
      assertEquals(0, c4.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals("v3", remoteCache.get("k3"));
      assertEquals("v4", remoteCache.get("k4"));

      hotRodServer4.stop();

      try {
         remoteCache.put("k5", "v1");
         remoteCache.put("k6", "v2");
         remoteCache.put("k7", "v3");
         remoteCache.put("k8", "v4");
      } catch (Exception e) {
    	  throw new RuntimeException(e);
      }
      
      hotRodServer1.stop();
      
      boolean exceptionCaught = false;

      try {
         remoteCache.put("k9", "v1");
         remoteCache.put("k10", "v2");
         remoteCache.put("k11", "v3");
         remoteCache.put("k12", "v4");
      } catch (Exception e) {
    	 exceptionCaught = true;
      }
      
      assert exceptionCaught : "exception should've been caught because the server wasn't removed from the list";
      
      List<SocketAddress> serverAddresses = new ArrayList<SocketAddress>();
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer2.getPort()));
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer3.getPort()));

      AlwaysFirstBalancingStrategy balancer = getBalancer();
      balancer.setServers(serverAddresses);
      
      try {
          remoteCache.put("k9", "v1");
          remoteCache.put("k10", "v2");
          remoteCache.put("k11", "v3");
          remoteCache.put("k12", "v4");
       } catch (Exception e) {
     	 throw new RuntimeException(e);
       }
   }
   
   @Test(dependsOnMethods = "testStopServer")
   public void testRemoveServers() {
      List<SocketAddress> serverAddresses = new ArrayList<SocketAddress>();
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer2.getPort()));
      serverAddresses.add(new InetSocketAddress("localhost", hotRodServer3.getPort()));

      AlwaysFirstBalancingStrategy balancer = getBalancer();
      balancer.setServers(serverAddresses);
      
      remoteCache.put("k1", "v1");
      remoteCache.put("k2", "v2");
      remoteCache.put("k3", "v3");
      remoteCache.put("k4", "v4");
      
      assertEquals(0, c1.size());
      assertEquals(4, c2.size());
      assertEquals(0, c3.size());
      assertEquals(0, c4.size());

      assertEquals("v1", remoteCache.get("k1"));
      assertEquals("v2", remoteCache.get("k2"));
      assertEquals("v3", remoteCache.get("k3"));
      assertEquals("v4", remoteCache.get("k4"));

      remoteCache.put("k5", "v2");
      remoteCache.put("k6", "v3");
      remoteCache.put("k7", "v1");
      remoteCache.put("k8", "v2");
      remoteCache.put("k9", "v3");
      remoteCache.put("k10", "v3");
      remoteCache.put("k11", "v3");
      remoteCache.put("k12", "v3");

      assertEquals(0, c1.size());
      assertEquals(12, c2.size());
      assertEquals(0, c3.size());
      assertEquals(0, c4.size());

   }
   
   private AlwaysFirstBalancingStrategy getBalancer() {
      TcpTransportFactory transportFactory = (TcpTransportFactory) TestingUtil.extractField(remoteCache.getRemoteCacheManager(), "transportFactory");
      return (AlwaysFirstBalancingStrategy) TestingUtil.extractField(transportFactory, "balancer");
   }

}
