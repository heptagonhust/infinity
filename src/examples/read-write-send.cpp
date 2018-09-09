/**
 * Examples - Read/Write/Send Operations
 *
 * (c) 2018 Claude Barthels, ETH Zurich
 * Contact: claudeb@inf.ethz.ch
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <cassert>

#include <infinity/core/Context.h>
#include <infinity/queues/QueuePairFactory.h>
#include <infinity/queues/QueuePair.h>
#include <infinity/memory/Buffer.h>
#include <infinity/memory/RegionToken.h>
#include <infinity/requests/RequestToken.h>

#define PORT_NUMBER 8011

int main(int argc, char **argv) {

	bool isServer = argc == 1;
	char *serverIp = NULL;

	if(isServer) {
		printf("./this for server and ./this [ServerIP] for client\n");
	}
	else {
		serverIp = argv[1];
		printf("client\n");
	}

	infinity::core::Context *context = new infinity::core::Context();
	infinity::queues::QueuePairFactory *qpFactory = new  infinity::queues::QueuePairFactory(context);
	infinity::queues::QueuePair *qp;

	if(isServer) {

		printf("Creating buffers to read from and write to\n");
		infinity::memory::Buffer *bufferToReadWrite = new infinity::memory::Buffer(context, 128 * sizeof(char));
		infinity::memory::RegionToken *bufferToken = bufferToReadWrite->createRegionToken();

		printf("Creating buffers to receive a message\n");
		infinity::memory::Buffer *bufferToReceive = new infinity::memory::Buffer(context, 128 * sizeof(char));
		context->postReceiveBuffer(bufferToReceive);

		printf("Setting up connection (blocking)\n");
		qpFactory->bindToPort(PORT_NUMBER);
		qp = qpFactory->acceptIncomingConnection(bufferToken, sizeof(infinity::memory::RegionToken));

		printf("Waiting for message (blocking)\n");
		infinity::core::receive_element_t receiveElement;
		while(!context->receive(&receiveElement));


		printf("Message received:\n");
		printf("%.128s", bufferToReceive->getData());
		printf("Message written:\n");
		printf("%.128s", bufferToReadWrite->getData());

        printf("Now resize and read again\n");
        bufferToReadWrite->resize(1024 * sizeof(char));

		printf("Waiting for message (blocking)\n");
		infinity::core::receive_element_t receiveElement;
		while(!context->receive(&receiveElement));

		printf("Message received:\n");
		printf("%.128s", bufferToReceive->getData());
		printf("Message written:\n");
		printf("%.128s", bufferToReadWrite->getData());

		delete bufferToReadWrite;
		delete bufferToReceive;

	} else {

		printf("Connecting to remote node\n");
		qp = qpFactory->connectToRemoteHost(serverIp, PORT_NUMBER);
		infinity::memory::RegionToken *remoteBufferToken = (infinity::memory::RegionToken *) qp->getUserData();


		printf("Creating buffers\n");
		infinity::memory::Buffer *buffer1Sided = new infinity::memory::Buffer(context, 128 * sizeof(char));
		infinity::memory::Buffer *buffer2Sided = new infinity::memory::Buffer(context, 128 * sizeof(char));

		strcpy((char*)buffer2Sided->getData(), "Fucking");

		printf("Reading content from remote buffer\n");
		infinity::requests::RequestToken requestToken(context);
		qp->read(buffer1Sided, remoteBufferToken, &requestToken);
		requestToken.waitUntilCompleted();

		strcpy((char*)buffer1Sided->getData(), "Hello");
		printf("Writing content to remote buffer\n");
		qp->write(buffer1Sided, remoteBufferToken, &requestToken);
		requestToken.waitUntilCompleted();

		printf("Sending message to remote host\n");
		qp->send(buffer2Sided, &requestToken);
		requestToken.waitUntilCompleted();

        buffer1Sided->resize(1024);
        memset(buffer1Sided->getData, 'f', 1024);
        buffer1Sided->getData[1023] = '\0';
		printf("Writing content to remote buffer\n");
		qp->write(buffer1Sided, remoteBufferToken, &requestToken);
		requestToken.waitUntilCompleted();

		printf("Sending message to remote host\n");
		qp->send(buffer2Sided, &requestToken);
		requestToken.waitUntilCompleted();

		delete buffer1Sided;
		delete buffer2Sided;
	}

	delete qp;
	delete qpFactory;
	delete context;

	return 0;

}
