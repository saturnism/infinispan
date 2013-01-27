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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.script.ScriptException;

import org.infinispan.CacheException;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

public class ScriptReducer<KOut, VOut> extends ScriptSupport implements Reducer<KOut, VOut> {
   /** The serialVersionUID */
   private static final long serialVersionUID = 6374161026723840881L;

   public ScriptReducer(String engineName, String script, Map<String, Object> variables) {
      super(engineName, script, variables);
   }

   public ScriptReducer(String engineName, String fileName, String script, Map<String, Object> variables) {
      super(engineName, fileName, script, variables);
   }

   public ScriptReducer(String engineName, String fileName, String script) {
      super(engineName, fileName, script);
   }

   public ScriptReducer(String engineName, String script) {
      super(engineName, script);
   }

   @SuppressWarnings("unchecked")
   @Override
   public VOut reduce(KOut reducedKey, Iterator<VOut> iter) {
      try {
         return (VOut) invocable.invokeFunction("reduce", reducedKey, iter);
      } catch (ScriptException e) {
         throw new CacheException("script exception", e);
      } catch (NoSuchMethodException e) {
         throw new CacheException("no such method", e);
      }
   }
   
   @SuppressWarnings("rawtypes")
   public static class Externalizer extends ExternalizerSupport<ScriptReducer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -6228476644468794619L;

      @SuppressWarnings("unchecked")
      @Override
      public Set<Class<? extends ScriptReducer>> getTypeClasses() {
         return Util.<Class<? extends ScriptReducer>>asSet(ScriptReducer.class);
      }

      @Override
      public Integer getId() {
         return Ids.SCRIPT_REDUCER;
      }

      @SuppressWarnings("unchecked")
      @Override
      protected ScriptReducer createObject(String engineName, String fileName, String script, Map<String, Object> variables) {
         return new ScriptReducer(engineName, fileName, script, variables);
      }
   }
}
