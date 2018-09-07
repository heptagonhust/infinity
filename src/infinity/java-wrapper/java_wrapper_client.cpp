
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
    // Create a queue pair
    //infinity::queues::QueuePairFactory *qpFactory = new infinity::queues::QueuePairFactory(context);
    //return addQpFactory(qpFactory);
}

JNIEXPORT void JNICALL stopRdma(JNIEnv *env, jobject)
{
    // Close connection//TODO RGY the sequence of delete
    delete_RegionToken();
    delete_qp();
    delete_qpFactory();
    delete_context();
    // delete remoteBufferToken;
    // delete localBuffer;
    // delete qp;
    // delete qpFactory;
    // delete context;
    // delete buffer2Sided;
}

//client part
//return remoteBufferTokenId//TODO int to uint16_t
//return qpId
JNIEXPORT int JNICALL clientConnect(JNIEnv *env, jobject, const char *ip, uint16_t port, int contextId)
{
    infinity::core::Context *context;
    context = whichContext(contextId);
    infinity::queues::QueuePairFactory *qpFactory = new infinity::queues::QueuePairFactory(context);
    infinity::queues::QueuePair *qp = qpFactory->connectToRemoteHost(ip, port);
    return addQp(qp);
}
//returm remoteBufferTokenId from a qp
JNIEXPORT int JNICALL clientInitRegionToken(JNIEnv *env, jobject, int qpId)
{
    infinity::queues::QueuePair *qp;
    qp = whichQp(qpId);
    infinity::memory::RegionToken *remoteBufferToken = (infinity::memory::RegionToken *)qp->getUserData();
    return addRegionToken(remoteBufferToken);
}

//jobject NewDirectByteBuffer(JNIEnv* env, void* address, jlong capacity);
//returen bufferId
JNIEXPORT int JNICALL initBuffer(JNIEnv *env, jobject, int contextId)
{
    printf("Creating buffers\n");
    infinity::core::Context *context;
    context = whichContext(contextId);
    infinity::memory::Buffer *buffer2Sided = new infinity::memory::Buffer(context, 128 * sizeof(char));
    return addBuffer(buffer2Sided);
    // jlong capacity = sizeof(buffer2Sided);
    // jobject directBuffer = env->NewDirectByteBuffer(buffer2Sided, capacity);
    // jobject globalRef = env->NewGlobalRef(directBuffer);
    // return globalRef;
}
//return the buffer
JNIEXPORT jobject JNICALL clientBuffer(JNIEnv *env, jobject, int bufferId)
{
    infinity::memory::Buffer *buffer2Sided = whichBuffer(bufferId);
    jlong capacity = sizeof(buffer2Sided);
    jobject directBuffer = env->NewDirectByteBuffer(buffer2Sided, capacity);
    jobject globalRef = env->NewGlobalRef(directBuffer);
    return globalRef;
}

JNIEXPORT void JNICALL rdmaWrite(JNIEnv *env, jobject, jobject localBuffer, int remoteBufferTokenId, int contextId, int qpId)
{
    // Write (one-sided) content of a local buffer to a remote buffer and wait for completion
    infinity::core::Context *context;
    context = whichContext(contextId);
    infinity::queues::QueuePair *qp;
    qp = whichQp(qpId);
    //infinity::memory::Buffer *localBuffer;
    //localBuffer = whichBuffer(localBufferId);
    infinity::memory::RegionToken *remoteBufferToken = whichRegionToken(remoteBufferTokenId);
    infinity::requests::RequestToken requestToken(context);
    qp->write((infinity::memory::Buffer *)localBuffer, (infinity::memory::RegionToken *)remoteBufferToken, &requestToken);
    requestToken.waitUntilCompleted();
}

JNIEXPORT void JNICALL rdmaRead(JNIEnv *env, jobject, int localBufferId, int remoteBufferTokenId, int contextId, int qpId)
{

    // Read (one-sided) from a remote buffer and wait for completion
    infinity::core::Context *context;
    context = whichContext(contextId);
    infinity::queues::QueuePair *qp;
    qp = whichQp(qpId);
    infinity::memory::Buffer *localBuffer;
    localBuffer = whichBuffer(localBufferId);
    infinity::memory::RegionToken *remoteBufferToken = whichRegionToken(remoteBufferTokenId);
    infinity::requests::RequestToken requestToken(context);
    qp->read((infinity::memory::Buffer *)localBuffer, (infinity::memory::RegionToken *)remoteBufferToken, &requestToken);
    requestToken.waitUntilCompleted();
}

JNIEXPORT void JNICALL rdmaSend(JNIEnv *env, jobject, int buffer2SidedId, int contextId, int qpId)
{
    // Send (two-sided) content of a local buffer over the queue pair and wait for completion
    infinity::core::Context *context;
    context = whichContext(contextId);
    infinity::queues::QueuePair *qp;
    qp = whichQp(qpId);
    infinity::requests::RequestToken requestToken(context);
    infinity::memory::Buffer *buffer2Sided = whichBuffer(buffer2SidedId);
    printf("Sending message to remote host\n");
    qp->send((infinity::memory::Buffer *)buffer2Sided, &requestToken);
    requestToken.waitUntilCompleted();
}
