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
package org.infinispan.groovy.mapreduce;

import groovy.lang.GroovyClassLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.Set;

import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.util.Util;

public class GroovyReducer<KOut, VOut> implements Reducer<KOut, VOut> {
   String script;
   private transient Reducer<KOut, VOut> reducer;
   
   public GroovyReducer(String script) {
      this.script = script;
      ClassLoader parent = getClass().getClassLoader();
      GroovyClassLoader groovyClassLoader = new GroovyClassLoader(parent);
      Class groovyClass = groovyClassLoader.parseClass(script);

      try {
         reducer = (Reducer) groovyClass.newInstance();
      } catch (InstantiationException e) {
         throw new IllegalArgumentException("script could not be instantiated", e);
      } catch (IllegalAccessException e) {
         throw new IllegalArgumentException("illegal access", e);
      }
   }

   @Override
   public VOut reduce(KOut reducedKey, Iterator<VOut> iter) {
      return reducer.reduce(reducedKey, iter);
   }
   
   public static class Externalizer extends AbstractExternalizer<GroovyReducer> {

      @Override
      public void writeObject(ObjectOutput output, GroovyReducer object) throws IOException {
         output.writeUTF(object.script);
      }

      @Override
      public GroovyReducer readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String script = input.readUTF();
         return new GroovyReducer(script);
      }

      @Override
      public Set<Class<? extends GroovyReducer>> getTypeClasses() {
         return Util.<Class<? extends GroovyReducer>>asSet(GroovyReducer.class);
      }
   }

}
