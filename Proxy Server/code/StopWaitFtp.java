import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.Arrays;

public class StopWaitFtp {
	private DatagramSocket udpSocket;		// UDP socket for sending and receiving segments
	private Socket tcpSocket;				// TCP socket for initial handshake
	private InetAddress serverAddress;
	private int serverUdpPort;
	private int serverTcpPort;

	private int initialSequenceNumber;
	private AtomicInteger ackExpectedSeqNum = new AtomicInteger();
	private byte[] receiveBuffer = new byte[FtpSegment.MAX_SEGMENT_SIZE];
	private AtomicInteger sequenceNumber = new AtomicInteger();	// Atomic integer for thread-safe sequence number handling

	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);	// Allow for simultaneous retransmission and timeout tasks
	private ScheduledFuture<?> ackListener;
	private ScheduledFuture<?> retransmissiontask;
	private final int timeout;
	private final Object lock = new Object();	// A lock object for synchronizing access

	private static final Logger logger = Logger.getLogger("StopWaitFtp"); // global logger
	private long lastPacketTime;

	private volatile boolean isSocketClosed = false;
	private volatile boolean ackReceived = true;


	/**
	 * Constructor to initialize the program
	 * @param serverName	The server's host name or IP address
	 * @param serverTcpPort	The server's TCP port for initial handshake
	 * @param timeout		The time-out interval for the retransmission timer, in milli-seconds
	 * @throws IOException	If an I/O error occurs during socket initialization
	 */
	public StopWaitFtp(int timeoutInterval) {
		this.timeout = timeoutInterval;
		try {
			this.serverAddress = InetAddress.getByName("localhost");
			this.serverTcpPort = 2025;
			this.tcpSocket = new Socket(this.serverAddress, this.serverTcpPort);    // Establish TCP connection for handsake
			this.udpSocket = new DatagramSocket();        // Open UDP socket for file transfer
			this.ackExpectedSeqNum.set(1);
			this.lastPacketTime = System.currentTimeMillis();
		} catch (IOException e) {
			throw new RuntimeException("Could not initialize StopWaitFtp: " + e.getMessage(), e);
		}
	}

	/**
	 * Method to exchange control information with the server over TCP
	 *
	 * @param fileName		Name of the file to be transferred
	 * @throws IOException 	If an I/O error occurs
	 */
	private void handshake(String fileName) throws IOException {
		// Establish strams for TCP Communication
		DataOutputStream outToServer = new DataOutputStream(tcpSocket.getOutputStream());
		DataInputStream inFromServer = new DataInputStream(tcpSocket.getInputStream());

		// 1. Send the name of the file as a UTF encoded string
		outToServer.writeUTF(fileName);

		// 2. Send the length of the file as a long value
		File file = new File(fileName);
		outToServer.writeLong(file.length());

		// 3. Send the local UDP port number used for file transfer as an int value
		outToServer.writeInt(udpSocket.getLocalPort());
		outToServer.flush();	// Ensure all data is sent

		// 4. Receive the server UDP port number used for file transfer as an int value
		serverUdpPort = inFromServer.readInt();

		// 5. Receive the initial sequence number used by the server as an int value
		initialSequenceNumber = inFromServer.readInt();

		System.out.println("Handshake complete. Server UDP Port: " + serverUdpPort + ", Initial Seq Num: " + initialSequenceNumber);
	}

	/**
	 * Send the specified file to the specified remote server.
	 *
	 * @param serverName	Name of the remote server
	 * @param serverPort	Port number of the remote server
	 * @param fileName		Name of the file to be trasferred to the rmeote server
	 * @return 				true if the file transfer completed successfully, false otherwise
	 */
	public boolean send(String serverName, int serverPort, String fileName) {
		try {
			this.serverAddress = InetAddress.getByName(serverName);
			this.udpSocket = new DatagramSocket();
			handshake(fileName);
			sendFileOverUdp(fileName);

			cleanup();
			return true;
		} catch (IOException e) {
			logger.severe("IOException during file transfer: " + e.getMessage());
			return false;
		} finally {
			cleanup();
		}
	}

	/**
	 * Method to send the specified file over UDP
	 *
	 * @param fileName		Name of the file to be transferred
	 * @throws IOException 	If an I/O error occurs
	 */
	private void sendFileOverUdp(String fileName) throws IOException {
		File file = new File(fileName);
		if (!file.exists()) {
			throw new FileNotFoundException("File not found: " + fileName);
		}

		try (FileInputStream fis = new FileInputStream(file);
			 BufferedInputStream bis = new BufferedInputStream(fis)) {
			byte[] buffer = new byte[FtpSegment.MAX_PAYLOAD_SIZE];
			int bytesRead;

			// Initialize sequence number
			sequenceNumber.set(initialSequenceNumber);

			// Read from the file and create segments until end of file
			while ((bytesRead = bis.read(buffer)) != -1) {
				// Adjust buffer size if last packet is smaller than the buffer
				byte[] segmentData = bytesRead < FtpSegment.MAX_PAYLOAD_SIZE ? Arrays.copyOf(buffer, bytesRead) : buffer;

				FtpSegment segment = new FtpSegment(sequenceNumber.getAndIncrement(), segmentData, bytesRead);
				sendAndAwaitAck(segment);
			}
		}
	}

	/**
	 * Method to send a segment and wait for an ACK, with retransmissions if necessary
	 *
	 * @param segment		Segment to be sent
	 * @throws IOException 	If an I/O error occurs
	 */
	private void sendAndAwaitAck(FtpSegment segment) throws IOException {
		ackReceived = false;	// Reset the flag everytime
		lastPacketTime = System.currentTimeMillis();

		ackExpectedSeqNum.incrementAndGet();

		// Start a task to listen for ACKs
		ackListener = executorService.scheduleAtFixedRate(() -> {
			DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
			try {
				udpSocket.receive(receivePacket);
				FtpSegment ackSegment = new FtpSegment(receivePacket);

				if (ackSegment.getSeqNum() == ackExpectedSeqNum.get()) {
					synchronized (lock) {
						ackReceived = true;
						System.out.println("ack " + (ackSegment.getSeqNum() - 1));
						lock.notifyAll(); // Notify the waiting send thread
					}
				} else {
					System.out.println("out-of-order ack " + ackSegment.getSeqNum());
				}
			} catch (IOException e) {
				logger.warning("IO/Exception while waiting for ACK: " + e.getMessage());
			}
		}, 0, timeout, TimeUnit.MILLISECONDS);

		// Send the segment
		sendSegment(segment); // Initial send

		// Wait for ACK or retransmit
		retransmissiontask = executorService.scheduleAtFixedRate(() -> {
			while (!ackReceived && !isSocketClosed && (System.currentTimeMillis() - lastPacketTime) < timeout) {
				try {
					lock.wait(timeout); // Wait for ACK or timeout
					if (!ackReceived) {
						System.out.println("timeout");
						System.out.println("retx " + segment.getSeqNum());
						sendSegment(segment); // Retransmit the segment
					}
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					logger.severe("ACK waiting thread interrupted: " + ie.getMessage());
				} catch (IOException e) {
					logger.warning("IO/Exception while retransmitting: " + e.getMessage());
				}
			}
		}, timeout, timeout, TimeUnit.MILLISECONDS);

		// After receiving the ACK or the socket is closed, cancel the listening task
		if (!ackListener.isCancelled()) {
			ackListener.cancel(true);
		}

		if (!retransmissiontask.isCancelled()) {
			retransmissiontask.cancel(true);
		}
	}

	/**
	 * Method to send a segment over UDP to the server
	 *
	 * @param segment		Segment to be sent
	 * @throws IOException 	If an I/O error occurs
	 */
	private void sendSegment(FtpSegment segment) throws IOException {
		if (isSocketClosed) {
			logger.warning("Attempt to send segment failed. Socket is closed");
			return;
		}
		// Serialize the FtpSegment object into a byte array
		byte[] segmentData = segment.toBytes();

		// Create a DatagramPacket with the serialized data, server address, and server UDP port
		DatagramPacket packet = new DatagramPacket(segmentData, segmentData.length, serverAddress, serverUdpPort);

		try {
			// Send the DatagramPacket over the UDP socket
			udpSocket.send(packet);
			System.out.println("send " + segment.getSeqNum());
		} catch (IOException e) {
			if (!isSocketClosed) {
				logger.severe("IOException while sending the segment: " + e.getMessage());
				isSocketClosed = true;
				throw e;
			}
		}
	}

	// Method to cleanly shutdown ScheduledExecutorService and close sockets
	private void cleanup() {
		// Set flag to indicate socket is about to be closed
		isSocketClosed = true;

		if (this.udpSocket != null && !udpSocket.isClosed()) {
			this.udpSocket.close();
		}
		if (this.tcpSocket != null && !this.tcpSocket.isClosed()) {
			try {
				this.tcpSocket.close();
			} catch (IOException e) {
				logger.severe("Error closing TCP socket: " + e.getMessage());
			}
		}
		executorService.shutdownNow();
		try {
			if (!executorService.awaitTermination(timeout, TimeUnit.MILLISECONDS)) {
				executorService.shutdownNow();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();	// Preserve interrupt status
			logger.severe("Executor service shutdown interrupted" + e.getMessage());
		}
	}
}