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

core::Context *context = nullptr;
queues::QueuePairFactory *qpFactory = nullptr;

#ifdef RDEBUG
#define rdma_debug std::cerr << "RdmaNative Debug: "
#else
#define rdma_debug                                                                                                             \
    if (false)                                                                                                                 \
    std::cerr
#endif
#define rdma_error std::cerr << "RdmaNative: "

template <typename T> inline void checkedDelete(T *&ptr) {
    if (ptr)
        delete ptr;
    ptr = nullptr;
}

class CRdmaServerConnectionInfo {
  private:
    //               < msgSize, DyBufTokenBufToken >
    typedef std::pair<uint32_t, memory::RegionToken> magicAndTokenType;
    queues::QueuePair *pQP = nullptr; // must delete

    memory::Buffer *pDynamicBufferTokenBuffer = nullptr;           // must delete
    memory::RegionToken *pDynamicBufferTokenBufferToken = nullptr; // must delete
    magicAndTokenType magicAndToken;

    memory::Buffer *pDynamicBuffer = nullptr;           // must delete
    memory::RegionToken *pDynamicBufferToken = nullptr; // must delete
    long currentSize = 4096;                            // Default 4K

    void initFixedLocalBuffer() {
        pDynamicBuffer = new memory::Buffer(context, currentSize);
        pDynamicBufferToken = pDynamicBuffer->createRegionToken();
        pDynamicBufferTokenBuffer = new memory::Buffer(context, sizeof(memory::RegionToken));
        pDynamicBufferTokenBufferToken = pDynamicBufferTokenBuffer->createRegionToken();
        magicAndToken.first = 0x00000000;
        magicAndToken.second = *pDynamicBufferTokenBufferToken;
    }

  public:
    //    CRdmaServerConnectionInfo() : pDynamicBuffer(nullptr), pDynamicBufferToken(nullptr),
    //        pDynamicBufferTokenBuffer(nullptr), pDynamicBufferTokenBufferToken(nullptr),
    //        currentSize(4096), pQP(nullptr) {
    //
    //        }
    ~CRdmaServerConnectionInfo() {
        checkedDelete(pQP);
        checkedDelete(pDynamicBuffer);
        checkedDelete(pDynamicBufferToken);
        checkedDelete(pDynamicBufferTokenBuffer);
        checkedDelete(pDynamicBufferTokenBufferToken);
    }

    void waitAndAccept() {
        pQP = qpFactory->acceptIncomingConnection(&magicAndToken, sizeof(magicAndToken));
        long cliQueryLength(*reinterpret_cast<long *>(pQP->getUserData()));
        rdma_debug << "accepted. clientQueryLength is " << cliQueryLength << std::endl;
    }

    bool isQueryReadable() { return magicAndToken.first == 0xaaaaaaaa; }

    void readQuery(void *&dataPtr, long &dataSize) {
        if (magicAndToken.first != 0xaaaaaaaa)
            throw std::runtime_error(std::string("read query: wrong magic. Want 0xaaaaaaaa, got ") +
                                     std::to_string(magicAndToken.first));
        dataPtr = pDynamicBuffer->getData();
        dataSize = pDynamicBuffer->getSizeInBytes();
    }

    void writeResponse(const void *dataPtr, long dataSize) {
        if (magicAndToken.first != 0xaaaaaaaa)
            throw std::runtime_error(std::string("write response: wrong magic. Want 0xaaaaaaaa, got ") +
                                     std::to_string(magicAndToken.first));
        pDynamicBuffer->resize(dataSize);
        // TODO: here's an extra copy. use dataPtr directly and jni global reference to avoid it!
        std::memcpy(pDynamicBuffer->getData(), dataPtr, dataSize);
        if (0xaaaaaaaa != std::exchange(magicAndToken.first, 0x55555555)) // not atomic
            throw std::runtime_error("write response: magic is changed while copying memory data.");
    }
};

class CRdmaClientConnectionInfo {

public:
    void connectToRemote(const char *serverAddr, int serverPort) {

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
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaConnect(JNIEnv *env, jobject, jstring jServerAddr, jint jServerPort) {
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
        env->SetIntField(jConn, jFieldErrCode, code);                                                                               \
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
        if(jFieldCxxPtr == NULL)
            REPORT_ERROR(5, "Unable to getFieldId `ptrCxxClass`");
        static_assert(sizeof(jlong) == sizeof(CRdmaClientConnectionInfo *), "jlong must have same size with C++ Pointer");
        env->SetLongField(jConn, jFieldCxxPtr, (jlong)pConn);
        env->SetIntField(jConn, jFieldErrCode, 0);
    }
    catch(std::exception *e) {
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
    }
    catch(std::exception *ex) {
        REPORT_FATAL("Failed to bind to port " << std::to_string(jListenPort) << ex.what());
    }
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
        if(jFieldCxxPtr == NULL)
            REPORT_ERROR(5, "Unable to getFieldId `ptrCxxClass`");
        static_assert(sizeof(jlong) == sizeof(CRdmaClientConnectionInfo *), "jlong must have same size with C++ Pointer");
        env->SetLongField(jConn, jFieldCxxPtr, (jlong)pConn);
        env->SetIntField(jConn, jFieldErrCode, 0);
    }
    catch(std::exception *e) {
        REPORT_ERROR(4, e.what());
    }
    return jConn;
}

