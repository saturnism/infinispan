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
package org.infinispan.distexec.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "groovy.BaseScriptWordCountMapReduceTest")
public abstract class BaseScriptWordCountMapReduceTest extends BaseWordCountMapReduceTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), true);
      createClusteredCaches(2, cacheName(), builder);
   }
   
   protected abstract String getEngineName();
   protected abstract String getWordCountMapperScriptName();
   protected abstract String getWordCountReducerScriptName();

   @Override
   public MapReduceTask<String, String, String, Integer> invokeMapReduce(String keys[], boolean useCombiner)
         throws Exception {
      return invokeMapReduce(keys, createWordCountMapper(), createWordCountReducer(), useCombiner);
   }

   protected String loadScript(String resource) {
      try {
         InputStream is = getClass().getClassLoader().getResourceAsStream(resource);
         BufferedReader br = new BufferedReader(new InputStreamReader(is));

         StringBuilder sb = new StringBuilder();

         String line;
         while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append("\n"); // newline, apparently, is important
         }
         br.close();
         return sb.toString();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
   
   @Override
   public void testMapperReducerIsolation() throws Exception {
      // this test doesn't really execute in the super class...
   }
   
   protected Mapper<String, String, String, Integer> createWordCountMapper() {
      return new ScriptMapper<String, String, String, Integer>(
            getEngineName(),
            getWordCountMapperScriptName(),
            loadScript(getWordCountMapperScriptName()));
   }

   protected Reducer<String, Integer> createWordCountReducer() {
      return new ScriptReducer<String, Integer>(
            getEngineName(),
            getWordCountReducerScriptName(),
            loadScript(getWordCountReducerScriptName()));
   }
}