/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.codahale.metrics.Timer

import org.apache.spark.internal.Logging

/**
 * An event bus which posts events to its listeners.
 * L代表监听器的泛型参数，表示支持任何类型的监听器
 * E代表事件的泛型参数
 */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

  // 维护所有注册的监听器
  private[this] val listenersPlusTimers = new CopyOnWriteArrayList[(L, Option[Timer])]

  // Marked `private[spark]` for access in tests.
  private[spark] def listeners = listenersPlusTimers.asScala.map(_._1).asJava

  /**
   * Returns a CodaHale metrics Timer for measuring the listener's event processing time.
   * This method is intended to be overridden by subclasses.
   */
  protected def getTimer(listener: L): Option[Timer] = None

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
   */
  final def addListener(listener: L): Unit = {
    listenersPlusTimers.add((listener, getTimer(listener)))
  }

  /**
   * Remove a listener and it won't receive any events. This method is thread-safe and can be called
   * in any thread.
   */
  final def removeListener(listener: L): Unit = {
    listenersPlusTimers.asScala.find(_._1 eq listener).foreach { listenerAndTimer =>
      listenersPlusTimers.remove(listenerAndTimer)
    }
  }

  /**
   * This can be overridden by subclasses if there is any extra cleanup to do when removing a
   * listener.  In particular AsyncEventQueues can clean up queues in the LiveListenerBus.
   */
  def removeListenerOnError(listener: L): Unit = {
    removeListener(listener)
  }


  /**
   * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
   * `postToAll` in the same thread for all events.
   * 此方法的作用是将事件投递给所有的监听器。
   */
  def postToAll(event: E): Unit = {
    // JavaConverters can create a JIterableWrapper if we use asScala.
    // However, this method will be called frequently. To avoid the wrapper cost, here we use
    // Java Iterator directly.
    val iter = listenersPlusTimers.iterator
    while (iter.hasNext) {
      val listenerAndMaybeTimer = iter.next()
      val listener = listenerAndMaybeTimer._1
      val maybeTimer = listenerAndMaybeTimer._2
      val maybeTimerContext = if (maybeTimer.isDefined) {
        maybeTimer.get.time()
      } else {
        null
      }
      try {
        doPostEvent(listener, event)
        if (Thread.interrupted()) {
          // We want to throw the InterruptedException right away so we can associate the interrupt
          // with this listener, as opposed to waiting for a queue.take() etc. to detect it.
          throw new InterruptedException()
        }
      } catch {
        case ie: InterruptedException =>
          logError(s"Interrupted while posting to ${Utils.getFormattedClassName(listener)}.  " +
            s"Removing that listener.", ie)
          removeListenerOnError(listener)
        case NonFatal(e) if !isIgnorableException(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      } finally {
        if (maybeTimerContext != null) {
          maybeTimerContext.stop()
        }
      }
    }
  }

  /**
   * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
   * thread for all listeners.
   * 用于将事件投递给指定的监听器，需子类实现。
   */
  protected def doPostEvent(listener: L, event: E): Unit

  /** Allows bus implementations to prevent error logging for certain exceptions. */
  protected def isIgnorableException(e: Throwable): Boolean = false

  // 查找与指定类型相同的监听器列表。
  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

}
