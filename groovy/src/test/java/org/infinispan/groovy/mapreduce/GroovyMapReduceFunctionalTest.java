package org.infinispan.groovy.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distexec.mapreduce.BaseWordCountMapReduceTest;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

@Test(groups = "functional", testName = "groovy.DistributedFourNodesMapReduceTest")
public class GroovyMapReduceFunctionalTest extends BaseWordCountMapReduceTest {

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.serialization().addAdvancedExternalizer(5555, new GroovyMapper.Externalizer())
            .addAdvancedExternalizer(5556, new GroovyReducer.Externalizer());
      ConfigurationBuilder defaultCacheConfig = TestCacheManagerFactory.getDefaultCacheConfiguration(false);

      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(gcb, defaultCacheConfig, flags);
      cacheManagers.add(cm);
      return cm;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(4, cacheName(), builder);
   }

   @Override
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[], boolean useCombiner)
         throws Exception {
      return invokeMapReduce(keys, createWordCountMapper(), createWordCountReducer(), useCombiner);
   }

   protected String loadGroovyScript(String resource) {
      try {
         InputStream is = getClass().getClassLoader().getResourceAsStream(resource);
         BufferedReader br = new BufferedReader(new InputStreamReader(is));

         StringBuilder sb = new StringBuilder();

         String line;
         while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append("\n");
         }
         br.close();
         return sb.toString();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void testMapperReducerIsolation() throws Exception {
      invokeMapReduce(null, createIsolationMapper(), createIsolationReducer(), false);
   }

   protected Mapper<String, String, String, Integer> createIsolationMapper() {
      return new GroovyMapper<String, String, String, Integer>(
            loadGroovyScript("org/infinispan/groovy/mapreduce/IsolationMapper.groovy"));
   }

   protected Reducer<String, Integer> createIsolationReducer() {
      return new GroovyReducer<String, Integer>(
            loadGroovyScript("org/infinispan/groovy/mapreduce/IsolationReducer.groovy"));
   }

   protected Mapper<String, String, String, Integer> createWordCountMapper() {
      return new GroovyMapper<String, String, String, Integer>(
            loadGroovyScript("org/infinispan/groovy/mapreduce/WordCountMapper.groovy"));
   }

   protected Reducer<String, Integer> createWordCountReducer() {
      return new GroovyReducer<String, Integer>(
            loadGroovyScript("org/infinispan/groovy/mapreduce/WordCountReducer.groovy"));
   }
}
