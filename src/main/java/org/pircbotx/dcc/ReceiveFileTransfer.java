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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import org.pircbotx.PircBotX;
import org.pircbotx.dcc.DccHandler.PendingFileTransfer;
import org.pircbotx.exception.DccException;
import org.pircbotx.exception.DccException.Reason;

import lombok.extern.slf4j.Slf4j;

/**
 * Receive a file from a user and send acknowledgement. Report statistics about
 * the file and file transfer.
 */
@Slf4j
public class ReceiveFileTransfer extends FileTransfer {

	SendFileTransferAcknowlegement acknowledge;

	public ReceiveFileTransfer(PircBotX bot, DccHandler dccHandler, PendingFileTransfer pendingFileTransfer,
			File file) {
		super(bot, dccHandler, pendingFileTransfer, file);
	}

	@Override
	protected void transferFile() {

		ByteBuffer buffer = ByteBuffer.allocateDirect(configuration.getDccPacketSize());

		try (SocketChannel inChannel = socket.getChannel();
				RandomAccessFile outputStream = new RandomAccessFile(file, "rw");
				FileChannel outChannel = outputStream.getChannel();) {

			acknowledge = new SendFileTransferAcknowlegement(inChannel, fileTransferStatus.startPosition);
			acknowledge.start();
			fileTransferStatus.start();

			outChannel.position(fileTransferStatus.startPosition);
			while (outChannel.position() < fileTransferStatus.fileSize) {
				if (dccHandler.shuttingDown || fileTransferStatus.dccState == DccState.SHUTDOWN) {
					break;
				}

				buffer.clear();
				long remaining = fileTransferStatus.fileSize - outChannel.position();
				if (remaining < buffer.capacity()) {
					buffer.limit((int) remaining);
				}

				int bytesRead = inChannel.read(buffer);
				if (bytesRead < 0) {
					break;
				}

				buffer.flip();
				outChannel.write(buffer);

				long currentPosition = outChannel.position();
				fileTransferStatus.bytesTransfered = currentPosition;
				fileTransferStatus.bytesAcknowledged = acknowledge.update(currentPosition);
			}

			// Signal ACK thread to send the final position and stop
			acknowledge.running = false;
			acknowledge.update(outChannel.position());
			try {
				acknowledge.join(5000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}

			fileTransferStatus.dccState = DccState.WAITING;
			log.info("Receive file transfer of file {} entered {} state for server to close the socket", file.getName(),
					fileTransferStatus.dccState);
			try {
				fileTransferStatus.interrupt();
				fileTransferStatus.join();

				fileTransferStatus.dccState = DccState.DONE;

			} catch (InterruptedException e) {
				fileTransferStatus.dccState = DccState.ERROR;
				log.error(
						"Receive file transfer of file {} failed to clean up gracefully! Please report this error with logs.",
						file.getName(), e);
			}
		} catch (IOException e) {
			fileTransferStatus.dccState = DccState.ERROR;
			fileTransferStatus.exception = new DccException(Reason.FILE_TRANSFER_CANCELLED, user, "User closed socket",
					e);
		} finally {

			log.info("Receive file transfer of file {} ended with state {}", file.getName(),
					fileTransferStatus.dccState);

		}
	}
}
