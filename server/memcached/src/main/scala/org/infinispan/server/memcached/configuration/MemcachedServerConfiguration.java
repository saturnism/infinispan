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
package org.infinispan.server.memcached.configuration;

import org.infinispan.configuration.BuiltBy;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

/**
 * MemcachedServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@BuiltBy(MemcachedServerConfigurationBuilder.class)
public class MemcachedServerConfiguration extends ProtocolServerConfiguration {
   private final String cache;

   MemcachedServerConfiguration(String cache, String name, String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads) {
      super(name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, workerThreads);
      this.cache = cache;
   }

   public String cache() {
      return cache;
   }

   @Override
   public String toString() {
      return "MemcachedServerConfiguration [cache=" + cache + ", " + super.toString() + "]";
   }
}
