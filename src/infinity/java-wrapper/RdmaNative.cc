#include "org_apache_hadoop_hbase_ipc_RdmaNative.h"
#include "org_apache_hadoop_hbase_ipc_RdmaNative_RdmaClientConnection.h"
#include "org_apache_hadoop_hbase_ipc_RdmaNative_RdmaServerConnection.h"

#include <infinity/core/Context.h>
#include <infinity/memory/Buffer.h>
#include <infinity/memory/RegionToken.h>
#include <infinity/queues/QueuePair.h>
#include <infinity/queues/QueuePairFactory.h>
#include <infinity/requests/RequestToken.h>
using namespace infinity;

#include <cstdint>
#include <cstring>
#include <iostream>

#ifdef RDEBUG
#define rdma_debug std::cerr << "RdmaNative Debug: "
#else
#define rdma_debug                                                                                                             \
    if (false)                                                                                                                 \
    std::cerr
#endif
#define rdma_error std::cerr << "RdmaNative: "

#ifndef uint32_t
#define uint32_t unsigned int
#endif
#ifndef uint64_t
#define uint64_t unsigned long long
#endif

template <typename T> inline void checkedDelete(T *&ptr) {
    if (ptr)
        delete ptr;
    ptr = nullptr;
}

core::Context *context = nullptr;
queues::QueuePairFactory *qpFactory = nullptr;

enum magic_t : uint32_t {
    MAGIC_CONNECTED = 0x00000000,
    MAGIC_SERVER_BUFFER_READY = 0xffffffff,
    MAGIC_QUERY_WROTE = 0xaaaaaaaa,
    MAGIC_RESPONSE_READY = 0x55555555
};
struct ServerStatusType {
    magic_t magic;
    volatile uint64_t currentQueryLength;
    memory::RegionToken dynamicBufferToken;
};
typedef memory::RegionToken DynamicBufferTokenBufferTokenType;

class CRdmaServerConnectionInfo {
  private:
    queues::QueuePair *pQP = nullptr; // must delete

    memory::Buffer *pDynamicBufferTokenBuffer = nullptr;           // must delete
    memory::RegionToken *pDynamicBufferTokenBufferToken = nullptr; // must delete
#define pServerStatus ((ServerStatusType *)pDynamicBufferTokenBuffer->getData())

    memory::Buffer *pDynamicBuffer = nullptr;           // must delete
    memory::RegionToken *pDynamicBufferToken = nullptr; // must delete
    uint64_t currentSize = 4096;                        // Default 4K

    void initFixedLocalBuffer() {
        pDynamicBuffer = new memory::Buffer(context, currentSize);
        pDynamicBufferToken = pDynamicBuffer->createRegionToken();
        pDynamicBufferTokenBuffer = new memory::Buffer(context, sizeof(ServerStatusType));
        pDynamicBufferTokenBufferToken = pDynamicBufferTokenBuffer->createRegionToken();
        pServerStatus->magic = MAGIC_CONNECTED;
        pServerStatus->currentQueryLength = 0;
    }

  public:
    ~CRdmaServerConnectionInfo() {
        checkedDelete(pQP);
        checkedDelete(pDynamicBuffer);
        checkedDelete(pDynamicBufferToken);
        checkedDelete(pDynamicBufferTokenBuffer);
    }

    void waitAndAccept() {
        initFixedLocalBuffer();
        pQP = qpFactory->acceptIncomingConnection(pDynamicBufferTokenBufferToken, sizeof(DynamicBufferTokenBufferTokenType));
    }

    bool isQueryReadable() {
        // Use this chance to check if client has told server its query size and allocate buffer.
        if (pServerStatus->magic == MAGIC_CONNECTED && pServerStatus->currentQueryLength != 0) {
        // Warning: pServerStatus->currentQueryLength may be under editing! The read value maybe broken!
        // So I set currentQueryLength as volatile and read it again.
        broken_value_read_again:
            uint64_t queryLength = pServerStatus->currentQueryLength;
            if (queryLength != pServerStatus->currentQueryLength)
                goto broken_value_read_again;

            if (queryLength > currentSize) {
                pDynamicBuffer->resize(queryLength);
                checkedDelete(pDynamicBufferToken);
                pDynamicBufferToken = pDynamicBuffer->createRegionTokenAt(&pServerStatus->dynamicBufferToken);
            }
            pServerStatus->magic == MAGIC_SERVER_BUFFER_READY;
        }
        return pServerStatus->magic == MAGIC_QUERY_WROTE;
    }

    void readQuery(void *&dataPtr, uint64_t &dataSize) {
        if (pServerStatus->magic != MAGIC_QUERY_WROTE)
            throw std::runtime_error(std::string("read query: wrong magic. Want 0xaaaaaaaa, got ") +
                                     std::to_string(pServerStatus->magic));
        dataPtr = pDynamicBuffer->getData();
        dataSize = pDynamicBuffer->getSizeInBytes();
    }

    void writeResponse(const void *dataPtr, uint64_t dataSize) {
        if (pServerStatus->magic != MAGIC_QUERY_WROTE)
            throw std::runtime_error(std::string("write response: wrong magic. Want 0xaaaaaaaa, got ") +
                                     std::to_string(pServerStatus->magic));
        pDynamicBuffer->resize(dataSize);
        // TODO: here's an extra copy. use dataPtr directly and jni global reference to avoid it!
        std::memcpy(pDynamicBuffer->getData(), dataPtr, dataSize);

        if (pServerStatus->magic != MAGIC_QUERY_WROTE)
            throw std::runtime_error("write response: magic is changed while copying memory data.");
        pServerStatus->magic = MAGIC_RESPONSE_READY;
    }
};

class CRdmaClientConnectionInfo {
    queues::QueuePair *pQP = nullptr;                                    // must delete
    memory::RegionToken *pRemoteDynamicBufferTokenBufferToken = nullptr; // must not delete
    uint64_t lastResponseSize = 4096; // If this query is smaller than last, do not wait for the server to allocate space.

    void rdmaSetServerMagic(magic_t magic) {
        // write the magic to MAGIC_QUERY_WROTE
        requests::RequestToken reqToken(context);
        memory::Buffer serverMagicBuffer(context, sizeof(ServerStatusType::magic));
        *(magic_t *)serverMagicBuffer.getData() = magic;
        static_assert(offsetof(ServerStatusType, magic) == 0, "Use read with more arg if offsetof(magic) is not 0.");
        pQP->write(&serverMagicBuffer, pRemoteDynamicBufferTokenBufferToken, sizeof(ServerStatusType::magic), &reqToken);
        reqToken.waitUntilCompleted();
    }

    magic_t rdmaGetServerMagic() {
        memory::Buffer serverMagicBuffer(context, sizeof(ServerStatusType::magic));
        requests::RequestToken reqToken(context);
        pQP->read(&serverMagicBuffer, pRemoteDynamicBufferTokenBufferToken, sizeof(ServerStatusType::magic), &reqToken);
        reqToken.waitUntilCompleted();
        return *(magic_t *)serverMagicBuffer.getData();
    }

  public:
    ~CRdmaClientConnectionInfo() { checkedDelete(pQP); }
    void connectToRemote(const char *serverAddr, int serverPort) {
        pQP = qpFactory->connectToRemoteHost(serverAddr, serverPort);
        pRemoteDynamicBufferTokenBufferToken = reinterpret_cast<memory::RegionToken *>(pQP->getUserData());
    }

    void writeQuery(void *dataPtr, uint64_t dataSize) {
        memory::Buffer wrappedDataBuffer(context, dataPtr, dataSize);
        memory::Buffer wrappedSizeBuffer(context, &dataSize, sizeof(dataSize));
#if __cplusplus < 201100L
        static_assert(std::is_pod<ServerStatusType>::value == true, "ServerStatusType must be pod to use C offsetof.");
#else
        static_assert(std::is_standard_layout<ServerStatusType>::value == true,
                      "ServerStatusType must be standard layout in cxx11 to use C offsetof.");
#endif
        requests::RequestToken reqToken(context);
        // write data size.
        pQP->write(&wrappedSizeBuffer, 0, pRemoteDynamicBufferTokenBufferToken, offsetof(ServerStatusType, currentQueryLength),
                   sizeof(dataSize), queues::OperationFlags(), &reqToken);
        reqToken.waitUntilCompleted();

        // Wait for the server allocating buffer...
        if (dataSize > lastResponseSize) {
            while (true) {
                static_assert(offsetof(ServerStatusType, magic) == 0, "Use read with more arg if offsetof(magic) is not 0.");
                if (MAGIC_SERVER_BUFFER_READY == rdmaGetServerMagic())
                    break; // Remote buffer is ready. Fire!
            }
        }
        // write the real data
        memory::Buffer tempTokenBuffer(context, sizeof(ServerStatusType));
        pQP->read(&tempTokenBuffer, pRemoteDynamicBufferTokenBufferToken, &reqToken);
        reqToken.waitUntilCompleted();
        memory::RegionToken remoteDynamicBufferToken = ((ServerStatusType *)tempTokenBuffer.getData())->dynamicBufferToken;
        pQP->write(&wrappedDataBuffer, &remoteDynamicBufferToken, &reqToken);
        reqToken.waitUntilCompleted();

        rdmaSetServerMagic(MAGIC_QUERY_WROTE);
    }

    bool isResponseReady() { return rdmaGetServerMagic() == MAGIC_RESPONSE_READY; }

    void readResponse(memory::Buffer *&pResponseDataBuf) {
        // Undefined behavior if the response is not ready.
        requests::RequestToken reqToken(context);
        memory::Buffer tempTokenBuffer(context, sizeof(ServerStatusType));
        pQP->read(&tempTokenBuffer, pRemoteDynamicBufferTokenBufferToken, &reqToken);
        reqToken.waitUntilCompleted();
        memory::RegionToken remoteDynamicBufferToken = ((ServerStatusType *)tempTokenBuffer.getData())->dynamicBufferToken;
        memory::Buffer *pResponseData = new memory::Buffer(context, remoteDynamicBufferToken.getSizeInBytes());
        pQP->read(pResponseData, &remoteDynamicBufferToken, &reqToken);

        // Set the server status to initial status after used!
        rdmaSetServerMagic(MAGIC_CONNECTED);

        pResponseDataBuf = pResponseData;
        lastResponseSize = pResponseData->getSizeInBytes();
    }
};

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaInitGlobal
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaInitGlobal(JNIEnv *, jobject) {
    try {
        context = new core::Context();
        qpFactory = new queues::QueuePairFactory(context);
    } catch (std::exception &ex) {
        rdma_error << "Exception: " << ex.what() << std::endl;
        checkedDelete(context);
        checkedDelete(qpFactory);
        return JNI_FALSE;
    }
    return JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaDestroyGlobal
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaDestroyGlobal(JNIEnv *, jobject) {
    checkedDelete(context);
    checkedDelete(qpFactory);
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaConnect
 * Signature: (Ljava/lang/String;I)Lorg/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaConnect(JNIEnv *env, jobject, jstring jServerAddr,
                                                                                  jint jServerPort) {
#define REPORT_FATAL(msg)                                                                                                      \
    do {                                                                                                                       \
        std::cerr << "RdmaNative FATAL: " << msg << "Unable to pass error to Java. Have to abort..." << std::endl;             \
        abort();                                                                                                               \
    } while (0)

    // Deal with java...
    static const jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection.");
    jmethodID jConnClsInit = env->GetMethodID(jConnCls, "<init>", "()V"); // -> problem!
    if (jConnClsInit == NULL)
        REPORT_FATAL("Unable to find constructor org/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection::<init> -> ()V.");
    jobject jConn = env->NewObject(jConnCls, jConnClsInit);
    if (jConn == NULL)
        REPORT_FATAL("Unable to create RdmaClientConnection object.");
    jfieldID jFieldErrCode = env->GetFieldID(jConnCls, "errorCode", "I");
    if (jFieldErrCode == NULL)
        REPORT_FATAL("Unable to getFieldId `errorCode` of class RdmaClientConnection.");

#define REPORT_ERROR(code, msg)                                                                                                \
    do {                                                                                                                       \
        env->SetIntField(jConn, jFieldErrCode, code);                                                                          \
        std::cerr << "RdmaNative ERROR: " << msg << "Returning error code to java..." << std::endl;                            \
    } while (0)

    jboolean isCopy;
    const char *serverAddr = env->GetStringUTFChars(jServerAddr, &isCopy);
    if (serverAddr == NULL)
        REPORT_ERROR(3, "GetStringUTFChars from jServerAddr error.");

    // do connect
    try {
        CRdmaClientConnectionInfo *pConn = new CRdmaClientConnectionInfo();
        pConn->connectToRemote(serverAddr, jServerPort);
        jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
        if (jFieldCxxPtr == NULL)
            REPORT_ERROR(5, "Unable to getFieldId `ptrCxxClass`");
        static_assert(sizeof(jlong) == sizeof(CRdmaClientConnectionInfo *), "jlong must have same size with C++ Pointer");
        env->SetLongField(jConn, jFieldCxxPtr, (jlong)pConn);
        env->SetIntField(jConn, jFieldErrCode, 0);
    } catch (std::exception &e) {
        REPORT_ERROR(4, e.what());
    }

    // cleanup
    if (isCopy == JNI_TRUE) {
        env->ReleaseStringUTFChars(jServerAddr, serverAddr);
    }
    return jConn;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaBind
 * Signature: (I)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaBind(JNIEnv *, jobject, jint jListenPort) {
    try {
        qpFactory->bindToPort(jListenPort);
    } catch (std::exception &e) {
        REPORT_FATAL("Failed to bind to port " << std::to_string(jListenPort) << e.what());
    }
    return JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaBlockedAccept
 * Signature: (I)Lorg/apache/hadoop/hbase/ipc/RdmaNative/RdmaServerConnection;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaBlockedAccept(JNIEnv *env, jobject) {
    // Deal with java...
    static const jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative/RdmaServerConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative/RdmaServerConnection.");
    jmethodID jConnClsInit = env->GetMethodID(jConnCls, "<init>", "()V"); // -> problem!
    if (jConnClsInit == NULL)
        REPORT_FATAL("Unable to find constructor org/apache/hadoop/hbase/ipc/RdmaNative/RdmaServerConnection::<init> -> ()V.");
    jobject jConn = env->NewObject(jConnCls, jConnClsInit);
    if (jConn == NULL)
        REPORT_FATAL("Unable to create RdmaServerConnection object.");
    jfieldID jFieldErrCode = env->GetFieldID(jConnCls, "errorCode", "I");
    if (jFieldErrCode == NULL)
        REPORT_FATAL("Unable to getFieldId `errorCode` of class RdmaServerConnection.");

    try {
        CRdmaServerConnectionInfo *pConn = new CRdmaServerConnectionInfo();
        pConn->waitAndAccept();
        jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
        if (jFieldCxxPtr == NULL)
            REPORT_ERROR(5, "Unable to getFieldId `ptrCxxClass`");
        static_assert(sizeof(jlong) == sizeof(CRdmaClientConnectionInfo *), "jlong must have same size with C++ Pointer");
        env->SetLongField(jConn, jFieldCxxPtr, (jlong)pConn);
        env->SetIntField(jConn, jFieldErrCode, 0);
    } catch (std::exception &e) {
        REPORT_ERROR(4, e.what());
    }
    return jConn;
}

////////////////////////////////////// RdmaClientConnection Methods
#undef REPORT_ERROR
#define REPORT_ERROR(msg)                                                                                                \
    do {                                                                                                                       \
        std::cerr << "RdmaNative ERROR: " << msg << "Returning error code to java..." << std::endl;                            \
        return JNI_FALSE; \
    } while (0)
#define REPORT_ERROR_B(msg)                                                                                                \
    do {                                                                                                                       \
        std::cerr << "RdmaNative ERROR: " << msg << "Returning error code to java..." << std::endl;                            \
        return NULL; \
    } while (0)



/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaClientConnection
 * Method:    isClosed
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaClientConnection_isClosed(JNIEnv *, jobject) {
    return JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaClientConnection
 * Method:    readResponse
 * Signature: ()Ljava/nio/ByteBuffer;
 */
memory::Buffer *previousResponseDataPtr = nullptr;
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaClientConnection_readResponse(JNIEnv *env, jobject self) {
    static const jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_ERROR_B("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaClientConnectionInfo *pConn = (CRdmaClientConnectionInfo *)cxxPtr;
    if(pConn == nullptr) REPORT_ERROR("cxx conn ptr is nullptr. is the connection closed?");

    checkedDelete(previousResponseDataPtr);
    try {
        pConn->readResponse(previousResponseDataPtr);
        if(previousResponseDataPtr == nullptr)
            throw std::runtime_error("readResponse return null");
    }
    catch(std::exception &e) {
        REPORT_ERROR_B(e.what());
    }
    return env->NewDirectByteBuffer(previousResponseDataPtr->getData(), previousResponseDataPtr->getSizeInBytes());
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaClientConnection
 * Method:    writeQuery
 * Signature: (Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaClientConnection_writeQuery(JNIEnv * env, jobject self,
                                                                                                            jobject jDataBuffer) {
    static const jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_ERROR("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaClientConnectionInfo *pConn = (CRdmaClientConnectionInfo *)cxxPtr;
    if(pConn == nullptr) REPORT_ERROR("cxx conn ptr is nullptr. is the connection closed?");

    void *tmpJAddr = env->GetDirectBufferAddress(jDataBuffer);
    if(tmpJAddr == NULL) REPORT_ERROR("jDataBuffer addr is null");
    try {
        pConn->writeQuery(tmpJAddr, env->GetDirectBufferCapacity(jDataBuffer));
    }
    catch(std::exception &e) {
        REPORT_ERROR(e.what());
    }

    return JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaClientConnection
 * Method:    close
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaClientConnection_close(JNIEnv *env, jobject self) {
    static const jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative/RdmaClientConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_ERROR("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaClientConnectionInfo *pConn = (CRdmaClientConnectionInfo *)cxxPtr;
    if(pConn == nullptr) REPORT_ERROR("cxx conn ptr is nullptr. is the connection closed?");

    checkedDelete(pConn);
    return JNI_TRUE;
}
