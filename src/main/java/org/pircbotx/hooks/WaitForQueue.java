/**
 * Copyright (C) 2010-2014 Leon Blakey <lord.quackstar at gmail.com>
 *
 * This file is part of PircBotX.
 *
 * PircBotX is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * PircBotX is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * PircBotX. If not, see <http://www.gnu.org/licenses/>.
 */
package org.pircbotx.hooks;

import lombok.NonNull;
import org.pircbotx.PircBotX;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Stores all events in a queue for processing. This is useful for sequential
 * processing of many similar events.
 * <p/>
 * Example:
 * <pre>
 * WaitForQueue queue = new WaitForQueue();
 * while(true) {
 *     MessageEvent mevent = queue.waitFor(MessageEvent.class);
 *     //Process event
 * }
 * queue.done();
 * </pre>
 *
 * @author Leon Blakey <lord.quackstar at gmail.com>
 */
public class WaitForQueue implements Closeable {
	protected final PircBotX bot;
	protected LinkedBlockingQueue<Event<PircBotX>> eventQueue = new LinkedBlockingQueue<Event<PircBotX>>();
	protected WaitForQueueListener listener;

	/**
	 * Create and store a queue listener in the specified bot's ListenerManager.
	 * It will be removed when {@link #done()} is called
	 *
	 * @param bot
	 */
	public WaitForQueue(@NonNull PircBotX bot) {
		this.bot = bot;
		bot.getConfiguration().getListenerManager().addListener(listener = new WaitForQueueListener());
	}

	/**
	 * Wait indefinitely for the specified event to be dispatched
	 *
	 * @param <E>        Event class
	 * @param eventClass The event class to wait for
	 * @return The event
	 * @throws InterruptedException
	 * @see #waitFor(java.util.List, long, java.util.concurrent.TimeUnit)
	 */
	public <E extends Event> E waitFor(@NonNull Class<E> eventClass) throws InterruptedException {
		List<Class<E>> eventList = new ArrayList<Class<E>>();
		eventList.add(eventClass);
		return (E) waitFor((List<Class<? extends Event>>) (Object) eventList);
	}

	/**
	 * Wait indefinitely for the one of the specified events to be dispatched
	 *
	 * @param eventClasses List of events to wait for
	 * @return One of the possible events
	 * @throws InterruptedException
	 * @see #waitFor(java.util.List, long, java.util.concurrent.TimeUnit)
	 */
	public Event waitFor(@NonNull Class<? extends Event>... eventClasses) throws InterruptedException {
		//Work around generics problems
		return waitFor(Arrays.asList(eventClasses));
	}

	/**
	 * Wait indefinitely for the one of the specified events to be dispatched
	 *
	 * @param eventClasses List of events to wait for
	 * @return One of the possible events
	 * @throws InterruptedException
	 * @see #waitFor(java.util.List, long, java.util.concurrent.TimeUnit)
	 */
	public Event waitFor(@NonNull List<Class<? extends Event>> eventClasses) throws InterruptedException {
		return waitFor(eventClasses, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
	}

	/**
	 * Wait for events of the specified event class to appear in the queue. If
	 * the event was dispatched before this is called, it will return
	 * immediately. Events that do not match the specified event class are
	 * discarded
	 *
	 * @param eventClasses Events to wait for
	 * @param timeout      Timeout value
	 * @param unit         Unit of timeout value
	 * @return One of the possible events
	 * @throws InterruptedException
	 */
	public Event waitFor(@NonNull List<Class<? extends Event>> eventClasses, long timeout, @NonNull TimeUnit unit) throws InterruptedException {
		while (true) {
			Event curEvent = eventQueue.poll(timeout, unit);
			for (Class<? extends Event> curEventClass : eventClasses)
				if (curEventClass.isInstance(curEvent))
					return curEvent;
		}
	}

	/**
	 * Shuts down the queue; VERY important to call when finished. Since this
	 * class stores every dispatched event, failure to close will eventually
	 * cause you to run out of memory
	 */
	@Override
	public void close() {
		bot.getConfiguration().getListenerManager().removeListener(listener);
		eventQueue.clear();
	}

	protected class WaitForQueueListener implements Listener {
		public void onEvent(Event event) throws Exception {
			eventQueue.add(event);
		}
	}
}
