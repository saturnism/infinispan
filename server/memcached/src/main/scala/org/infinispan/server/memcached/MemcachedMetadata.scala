/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.server.memcached

import org.infinispan.container.versioning.EntryVersion
import org.infinispan.metadata.{EmbeddedMetadata, Metadata}
import Metadata.Builder
import org.infinispan.marshall.AbstractExternalizer
import java.util
import java.io.{ObjectInput, ObjectOutput}
import scala.collection.JavaConversions.setAsJavaSet
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.{MILLISECONDS => MILLIS}
import org.jboss.marshalling.util.IdentityIntMap

/**
 * Memcached metadata information.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
class MemcachedMetadata(val flags: Long, val version: EntryVersion) extends Metadata {

   def lifespan(): Long = -1

   def maxIdle(): Long = -1

   def builder(): Builder = new MemcachedMetadataBuilder().flags(flags).version(version)

   override def equals(obj: Any): Boolean = {
      obj match {
         case that: MemcachedMetadata =>
            (that.canEqual(this)) &&
                    flags == that.flags &&
                    version == that.version
         case _ => false
      }
   }

   def canEqual(other: Any): Boolean = other.isInstanceOf[MemcachedMetadata]

   override def hashCode(): Int = 41 * (41 + flags.hashCode) + version.hashCode()

   override def toString: String = s"MemcachedMetadata(flags=$flags, version=$version)"

}

private class MemcachedExpirableMetadata(
        override val flags: Long, override val version: EntryVersion,
        lifespanTime: Long, lifespanUnit: TimeUnit,
        maxIdleTime: Long, maxIdleUnit: TimeUnit) extends MemcachedMetadata(flags, version) {

   override final val lifespan = lifespanUnit.toMillis(lifespanTime)

   override final val maxIdle = maxIdleUnit.toMillis(maxIdleTime)

   override def equals(obj: Any): Boolean = {
      obj match {
         case that: MemcachedExpirableMetadata =>
            (that.canEqual(this)) &&
                    flags == that.flags &&
                    version == that.version &&
                    lifespan == that.lifespan &&
                    maxIdle == that.maxIdle
         case _ => false
      }
   }

   override def canEqual(other: Any): Boolean = other.isInstanceOf[MemcachedExpirableMetadata]

   override def hashCode(): Int =
      41 * (41 * (41 * (41 + flags.hashCode) + version.hashCode) + lifespan.toInt) + maxIdle.toInt

   override def toString: String =
      s"MemcachedExpirableMetadata(flags=$flags, version=$version, lifespan=$lifespan, maxIdle=$maxIdle)"

}

private class MemcachedMetadataBuilder extends EmbeddedMetadata.Builder {

   private var flags: Long = _

   def flags(flags: Long): MemcachedMetadataBuilder = {
      this.flags = flags
      this
   }

   override def build(): Metadata =
      MemcachedMetadata(flags, version, lifespan, lifespanUnit, maxIdle, maxIdleUnit)

}


object MemcachedMetadata {

   def apply(flags: Long, version: EntryVersion,
           lifespan: Long, lifespanUnit: TimeUnit,
           maxIdle: Long, maxIdleUnit: TimeUnit): MemcachedMetadata = {
      if (lifespan < 0 && maxIdle < 0)
         new MemcachedMetadata(flags, version)
      else
         new MemcachedExpirableMetadata(flags, version, lifespan, lifespanUnit, maxIdle, maxIdleUnit)
   }

   def apply(flags: Long, version: EntryVersion): MemcachedMetadata =
      new MemcachedMetadata(flags, version)

   class Externalizer extends AbstractExternalizer[MemcachedMetadata] {

      final val Immortal = 0
      final val Expirable = 1

      final val numbers = new IdentityIntMap[Class[_]](2)

      numbers.put(classOf[MemcachedMetadata], Immortal)
      numbers.put(classOf[MemcachedExpirableMetadata], Expirable)

      def readObject(input: ObjectInput): MemcachedMetadata = {
         val flags = input.readLong()
         val version = input.readObject().asInstanceOf[EntryVersion]
         val number = input.readUnsignedByte()
         number match {
            case Immortal => MemcachedMetadata(flags, version)
            case Expirable =>
               val lifespan = input.readLong()
               val maxIdle = input.readLong()
               MemcachedMetadata(flags, version, lifespan, MILLIS, maxIdle, MILLIS)
         }
      }

      def writeObject(output: ObjectOutput, meta: MemcachedMetadata) {
         output.writeLong(meta.flags)
         output.writeObject(meta.version)
         val number = numbers.get(meta.getClass, -1)
         output.write(number)
         if (number == Expirable) {
            output.writeLong(meta.lifespan())
            output.writeLong(meta.maxIdle())
         }
      }

      def getTypeClasses: util.Set[Class[_ <: MemcachedMetadata]] =
         setAsJavaSet(Set[java.lang.Class[_ <: MemcachedMetadata]](
            classOf[MemcachedMetadata], classOf[MemcachedExpirableMetadata]))

   }

}