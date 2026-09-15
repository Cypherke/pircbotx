/*
 * Copyright (C) 2010-2022 The PircBotX Project Authors
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
package org.pircbotx.dcc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Semaphore;

import lombok.extern.slf4j.Slf4j;

/**
 * Send current number of bytes received from a file transfer.
 * Runs as a separate thread so that a blocked write never stalls the receive loop.
 */
@Slf4j
public class SendFileTransferAcknowlegement extends Thread {

	protected final SocketChannel socketChannel;
	protected volatile long positionToAck;
	protected volatile boolean running = true;
	private final Semaphore pending = new Semaphore(0);

	public SendFileTransferAcknowlegement(SocketChannel socketChannel, long startPosition) {
		this.socketChannel = socketChannel;
		this.positionToAck = startPosition;
		setDaemon(true);
	}

	/**
	 * Signal the ACK thread to send the given position.
	 * Returns the position for use as bytesAcknowledged.
	 */
	public long update(long position) {
		positionToAck = position;
		pending.release();
		return position;
	}

	@Override
	public void run() {
		try {
			while (running) {
				pending.acquire();
				pending.drainPermits();
				sendAcknowledge(positionToAck);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (IOException e) {
			log.debug("ACK send failed, socket likely closed: {}", e.getMessage());
		}
	}

	protected void sendAcknowledge(long position) throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt((int) position);
		buf.flip();
		while (buf.hasRemaining()) {
			socketChannel.write(buf);
		}
	}

}
