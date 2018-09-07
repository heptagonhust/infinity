#include <stdlib.h>
#include <unistd.h>
#include <cassert>

#include <infinity/core/Context.h>
#include <infinity/queues/QueuePairFactory.h>
#include <infinity/queues/QueuePair.h>
#include <infinity/memory/Buffer.h>
#include <infinity/memory/RegionToken.h>
#include <infinity/requests/RequestToken.h>
#define PORT_NUMBER 8011
#define SERVER_IP "192.0.0.1"


int main(){

// Create new context
infinity::core::Context *context = new infinity::core::Context();

// Create a queue pair
infinity::queues::QueuePairFactory *qpFactory = new  infinity::queues::QueuePairFactory(context);
infinity::queues::QueuePair *qp = qpFactory->connectToRemoteHost(SERVER_IP, PORT_NUMBER);

// Create and register a buffer with the network
infinity::memory::Buffer *localBuffer = new infinity::memory::Buffer(context, BUFFER_SIZE);

// Get information from a remote buffer
infinity::memory::RegionToken *remoteBufferToken = new infinity::memory::RegionToken(REMOTE_BUFFER_INFO);

// Read (one-sided) from a remote buffer and wait for completion
infinity::requests::RequestToken requestToken(context);
qp->read(localBuffer, remoteBufferToken, &requestToken);
requestToken.waitUntilCompleted();

// Write (one-sided) content of a local buffer to a remote buffer and wait for completion
qp->write(localBuffer, remoteBufferToken, &requestToken);
requestToken.waitUntilCompleted();

// Send (two-sided) content of a local buffer over the queue pair and wait for completion
qp->send(localBuffer, &requestToken);
requestToken.waitUntilCompleted();

// Close connection
delete remoteBufferToken;
delete localBuffer;
delete qp;
delete qpFactory;
delete context;
}