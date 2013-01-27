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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Map.Entry;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.infinispan.marshall.AdvancedExternalizer;

public class ScriptSupport {
   final String engineName;
   final String fileName;
   final String script;
   final Map<String, Object> variables;
   protected final transient Invocable invocable;
      
   public ScriptSupport(String engineName, String script) {
      this(engineName, null, script);
   }
   
   public ScriptSupport(String engineName, String fileName, String script) {
      this(engineName, fileName, script, null);
   }
   
   public ScriptSupport(String engineName, String script, Map<String, Object> variables) {
      this(engineName, null, script, variables);
   }
   
   public ScriptSupport(String engineName, String fileName, String script, Map<String, Object> variables) {
      if (engineName == null)
         throw new NullPointerException("engineName cannot be null");
      if (script == null)
         throw new NullPointerException("script cannot be null");
      
      ScriptEngineManager manager = new ScriptEngineManager();
      this.engineName = engineName;
      this.script = script;
      this.variables = variables;
      this.fileName = fileName;

      ScriptEngine engine = manager.getEngineByName(engineName);

      if (engine == null)
         throw new IllegalArgumentException("engine name '" + engineName + " is not found");
      
      if (fileName != null)
         engine.put(ScriptEngine.FILENAME, fileName);

      try {
         if (variables != null) {
            for (Entry<String, Object> entry : variables.entrySet()) {
               engine.put(entry.getKey(), entry.getValue());
            }
         }
         engine.eval(script);
         if (!(engine instanceof Invocable)) {
            throw new IllegalArgumentException("engine name '" + engineName
                  + "' does not implement Invocable interface");
         }

         invocable = (Invocable) engine;
      } catch (ScriptException e) {
         throw new IllegalArgumentException("script couldn't be evaluated", e);
      }

   }
   
   public abstract static class ExternalizerSupport<T extends ScriptSupport> implements AdvancedExternalizer<T> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 8834420819055541815L;
      
      @Override
      public void writeObject(ObjectOutput output, T object) throws IOException {
         output.writeUTF(object.engineName);
         output.writeUTF(object.fileName);
         output.writeUTF(object.script);
         output.writeObject(object.variables);
      }
      
      @SuppressWarnings("unchecked")
      @Override
      public T readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String engineName = input.readUTF();
         String fileName = input.readUTF();
         String script = input.readUTF();
         Map<String, Object> variables = (Map<String, Object>) input.readObject();
         return createObject(engineName, fileName, script, variables);
      }
      
      protected abstract T createObject(String engineName, String fileName, String script, Map<String, Object> variables);
   }
}
