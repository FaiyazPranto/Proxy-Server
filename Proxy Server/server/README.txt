
FtpServer

Assignment 4
CPSC 441

Author: Majid Ghaderi
Email: mghaderi@ucalgary.ca

 
Running the server:
==============
Use the following command to start the server:

    java -jar ftpserver.jar

To stop the server, type "quit".

Using the above command, the server starts with a set of default parameters. 
You can specify your own parameters using the command line options described below.
Normally, you do not need to change the default values for options -x, -t and -r.

The following options can be used to pass command line arguments to the server:

	-v 	Verbosity level of the server logs. 
		Available values are "all", "info" and "off".
		Passing "-v all" generates the most amount of logging messages, 
		while "-v off" turns off output messages completely.
			
	-p 	Server port number.
		It can be any integer greater than 1024, less than 64K.
			
	-i 	Initial sequence number to be used by the server. 
		The server communicates the initial sequence number to the client
		during the handshake process and expects the first segment arriving
		from the client to have this sequence number. 			
	
	-d 	Average additional delay added to acknowledgements by the server.
		The server uses this parameter to wait for some random delay before 
		sending each ACK. The actual delay is generated randomly so that the 
		average delay is equal to this parameter. Its value is specified in 
		milli-seconds.
	
	-l 	Segment loss probability.
		It specifies the ratio of lost segments at the server. The server randomly 
		drops arriving segments. The probability that the server drops a segment 
		is given by this parameter. It is specified by a float number between 0 and 1.

	-x 	The sequence number of the segment to be dropped by the server.
		This parameter is useful when observing the client response to 
		packet losses specially when setting "-l 0". This option can be very
		useful during testing and debugging, as it can force the server to
		always drop a specific packet.
	
	-t 	Connection idle time.
		If the server does not hear from the client during the specified idle time,
		it assumes that the client is gone and so will close the session. The time is
		specified in milli-seconds.

	-r 	Seed for the random number generator.
		The drop and delay modules use a random number generator.
		To reproduce the same results, use the the same seed every time
		the server starts. It can be any integer greater than 0.
