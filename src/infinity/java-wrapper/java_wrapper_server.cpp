

// JNIEXPORT jobject JNICALL Java_com_foo_allocNativeBuffer(JNIEnv* env, jobject thiz, jlong size)
// {
//     void* buffer = malloc(size);
//     jobject directBuffer = env->NewDirectByteBuffer(buffer, size);
//     jobject globalRef = env->NewGlobalRef(directBuffer);

//     return globalRef;
// }
//http://samplecodebank.blogspot.com/2013/04/jni-jnienv-newdirectbytebuffer-example.html

#include <jni.h>

//#include "/usr/lib/jvm/default-java/include/jni.h"

#include <stdlib.h>
#include <unistd.h>
#include <cassert>

#include "../../infinity/core/Context.h"
#include "../../infinity/queues/QueuePairFactory.h"
#include "../../infinity/queues/QueuePair.h"
#include "../../infinity/memory/Buffer.h"
#include "../../infinity/memory/RegionToken.h"
#include "../../infinity/requests/RequestToken.h"

infinity::core::Context *contextlinks[0]; //keep [0] empty
int contextNumber = 0;

infinity::queues::QueuePair *qplinks[0];
int qpNumber = 0;

infinity::queues::QueuePairFactory *qpFactorylinks[0];
int qpFactoryNumber = 0;

infinity::memory::RegionToken *RegionTokenlinks[0];
int RegionTokenNumber = 0;

infinity::memory::Buffer *Bufferlinks[0];
int BufferNumber = 0;

int addContext(infinity::core::Context *context)
{
    contextNumber++;
    contextlinks[contextNumber] = context;
    return contextNumber;
}
infinity::core::Context *whichContext(int contextId)
{
    infinity::core::Context *context;
    context = contextlinks[contextId];
    return context;
}
void delete_context()
{
    for (size_t i = 1; i < contextNumber; i++)
    {
        delete contextlinks[i];
    }
}

int addRegionToken(infinity::memory::RegionToken *RegionToken)
{
    RegionTokenNumber++;
    RegionTokenlinks[RegionTokenNumber] = RegionToken;
    return RegionTokenNumber;
}
infinity::memory::RegionToken *whichRegionToken(int RegionTokenId)
{
    infinity::memory::RegionToken *RegionToken;
    RegionToken = RegionTokenlinks[RegionTokenId];
    return RegionToken;
}
void delete_RegionToken()
{
    for (size_t i = 1; i < RegionTokenNumber; i++)
    {
        delete RegionTokenlinks[i];
    }
}

int addBuffer(infinity::memory::Buffer *Buffer)
{
    BufferNumber++;
    Bufferlinks[BufferNumber] = Buffer;
    return BufferNumber;
}
infinity::memory::Buffer *whichBuffer(int BufferId)
{
    infinity::memory::Buffer *Buffer;
    Buffer = Bufferlinks[BufferId];
    return Buffer;
}
void delete_Buffer()
{
    for (size_t i = 1; i < BufferNumber; i++)
    {
        delete Bufferlinks[i];
    }
}

int addQpFactory(infinity::queues::QueuePairFactory *qpFactory)
{
    qpFactoryNumber++;
    qpFactorylinks[qpFactoryNumber] = qpFactory;
    return qpFactoryNumber;
}

infinity::queues::QueuePairFactory *whichQpFactory(int qpFactoryId)
{
    infinity::queues::QueuePairFactory *qpFactory;
    qpFactory = qpFactorylinks[qpFactoryId];
    return qpFactory;
}

void delete_qpFactory()
{
    for (size_t i = 1; i < qpFactoryNumber; i++)
    {
        delete qpFactorylinks[i];
    }
}

int addQp(infinity::queues::QueuePair *qp)
{
    qpNumber++;
    qplinks[qpNumber] = qp;
    return qpNumber;
}

infinity::queues::QueuePair *whichQp(int qpId)
{
    infinity::queues::QueuePair *qp;
    qp = qplinks[qpId];
    return qp;
}

void delete_qp()
{

    for (size_t i = 1; i < qpNumber; i++)
    {
        delete qplinks[i];
    }
}

//should we return the struct or just a id of struct????
// 2 side run
//return a contextId
JNIEXPORT int JNICALL initcontext(JNIEnv *env, jobject)
{
    // Create new context
    infinity::core::Context *context = new infinity::core::Context();
    return addContext(context);
}
//return a qpFactoryID
JNIEXPORT int JNICALL initConnection(JNIEnv *env, jobject, int contextId)
{
    // Create new context
    infinity::core::Context *context;
    context = whichContext(contextId);
    // Create a queue pair
    infinity::queues::QueuePairFactory *qpFactory = new infinity::queues::QueuePairFactory(context);
    return addQpFactory(qpFactory);
}

JNIEXPORT void JNICALL stopRdma(JNIEnv *env, jobject)
{
    // Close connection
    delete_RegionToken();
    delete_qp();
    delete_qpFactory();
    delete_context();
}

//server side
//return bufferId
JNIEXPORT int JNICALL create2sideBuffer(JNIEnv *env, jobject, int contextId, int buffer_size)
{
    // Create and register a buffer with the network
    infinity::core::Context *context;
    context = whichContext(contextId);
    infinity::memory::Buffer *bufferToReadWrite = new infinity::memory::Buffer(context, 128 * sizeof(char));
    return addBuffer(bufferToReadWrite);
}
//return the buffer
JNIEXPORT jobject JNICALL serverBuffer(JNIEnv *env, jobject, int bufferId)
{
    infinity::memory::Buffer *bufferToReadWrite = whichBuffer(bufferId);
    jlong capacity = sizeof(bufferToReadWrite);
    jobject directBuffer = env->NewDirectByteBuffer(bufferToReadWrite, capacity);
    jobject globalRef = env->NewGlobalRef(directBuffer);
    return globalRef;
}

//return bufferTokenId
JNIEXPORT int JNICALL initBufferToken(JNIEnv *env, jobject, int bufferId)
{
    infinity::memory::Buffer *bufferToReadWrite = whichBuffer(bufferId);
    infinity::memory::RegionToken *bufferToken = bufferToReadWrite->createRegionToken();
    return addRegionToken(bufferToken);
}

JNIEXPORT void JNICALL waitForConn(JNIEnv *env, jobject, uint16_t port, int qpFactoryId, int bufferTokenId)
{
    printf("Setting up connection (blocking)\n");
    infinity::queues::QueuePairFactory *qpFactory = whichQpFactory(qpFactoryId);
    qpFactory->bindToPort(port);
    infinity::queues::QueuePair *qp;
    infinity::memory::RegionToken *bufferToken = whichRegionToken(bufferTokenId);
    qp = qpFactory->acceptIncomingConnection(bufferToken, sizeof(infinity::memory::RegionToken));
}

JNIEXPORT void JNICALL PollingForMes(JNIEnv *env, jobject, int contextId)
{
    infinity::core::Context *context;
    context = whichContext(contextId);
    printf("Waiting for message (blocking)\n");
    infinity::core::receive_element_t receiveElement;
    while (!context->receive(&receiveElement))
        ;
}