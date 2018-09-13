#include "org_apache_hadoop_hbase_ipc_RdmaNative.h"
#include "org_apache_hadoop_hbase_ipc_RdmaNative_RdmaConnection.h"

#include <infinity/core/Context.h>
#include <infinity/queues/QueuePairFactory.h>
#include <infinity/queues/QueuePair.h>
#include <infinity/memory/Buffer.h>
#include <infinity/memory/RegionToken.h>
#include <infinity/requests/RequestToken.h>
using namespace infinity;

#include <iostream>

core::Context *context;
queues::QueuePairFactory *qpFactory = new infinity::queues::QueuePairFactory(context);

#ifdef RDEBUG
#define rdma_debug std::cerr << "RdmaNative Debug: "
#else
#define rdma_debug if(false) std::cerr
#endif
#define rdma_error std::cerr << "RdmaNative: "

class CRdmaConnectionInfo {
private:
    /* 4ptr is maintained by C++ class.
    private long ptrQP;
    private long ptrRegionTokenBuf; // registered as fixed length while making conn.
    private long ptrRemoteSerialBuf; // remote buffer serial number.
    private long ptrDynamicDataBuf; // must register on every local write. Invalidate it only if ptrRegionTokenBuf has changed!
    */
    typedef std::pair<memory::RegionToken, memory::RegionToken> qpUserDataType;
    queues::QueuePair *pQP; // must delete

    memory::Buffer *pLocalRegionTokenBuffer; // must delete
    memory::RegionToken *pLocalRegionTokenBufferToken; // must delete
    memory::Buffer *pLocalDynamicRegionSizeBuffer; // must delete
    memory::RegionToken *pLocalDynamicRegionSizeBufferToken; // must delete

    memory::Buffer *pLocalBuffer; // dynamically managed
    memory::RegionToken *pLocalBufferToken;

    memory::RegionToken *pRemoteRegionTokenBufferToken; // must NOT delete
    memory::RegionToken *pRemoteDynamicRegionSizeBufferToken; // must NOT delete

    void initFixedLocalBuffer() {
		pLocalRegionTokenBuffer = new memory::Buffer(context, sizeof(infinity::memory::RegionToken));
		pLocalRegionTokenBufferToken = pLocalRegionTokenBuffer->createRegionToken();
        pLocalDynamicRegionSizeBuffer = new memory::Buffer(context, sizeof(long));
        pLocalDynamicRegionSizeBufferToken = pLocalDynamicRegionSizeBuffer->createRegionToken();
        *reinterpret_cast<long *>(pLocalDynamicRegionSizeBuffer->getData()) = 0;
    }
    void atomicSetLocalRegionTokenBuf(const memory::RegionToken &newToken) {

    }
public:
    CRdmaConnectionInfo() : pQP(nullptr), pLocalRegionTokenBuffer(nullptr), pLocalRegionTokenBufferToken(nullptr),
        pRemoteRegionTokenBufferToken(nullptr), pLocalDynamicRegionSizeBuffer(nullptr), pLocalDynamicRegionSizeBufferToken(nullptr),
        pLocalBuffer(nullptr), pLocalBufferToken(nullptr)
        {}
    ~CRdmaConnectionInfo() {
        if(pQP) delete pQP;
        if(pLocalRegionTokenBuffer) delete pLocalRegionTokenBuffer;
        if(pLocalRegionTokenBufferToken) delete pLocalRegionTokenBufferToken;
        if(pLocalDynamicRegionSizeBuffer) delete pLocalDynamicRegionSizeBuffer;
        if(pLocalDynamicRegionSizeBufferToken) delete pLocalDynamicRegionSizeBufferToken;
        if(pRemoteRegionTokenBufferToken) /*DO NOT DELETE IT!!!!!!!*/;
        if(pRemoteDynamicRegionSizeBufferToken) /*DO NOT DELETE IT!!!!!!!*/;
    }

    void localWrite(const void *dataPtr, long dataSize) {
        // TODO: add serial number
        memory::Buffer *pNewBuffer = new memory::Buffer(context, dataSize);
        memory::RegionToken *pNewBufferToken = pNewBuffer->createRegionToken();
        atomicSetLocalRegionTokenBuf
        
    }

    void connectToRemote(const char *serverAddr, int serverPort) {
        // Factory method
        initFixedLocalBuffer();
        qpUserDataType qpUserData(*pLocalRegionTokenBufferToken, *pLocalDynamicRegionSizeBufferToken);
        pQP = qpFactory->connectToRemoteHost(serverAddr, serverPort, &qpUserData, sizeof(qpUserData));
        qpUserDataType qpRemoteUserData (*reinterpret_cast<qpUserDataType *>(pQP->getUserData()));
        pRemoteRegionTokenBufferToken = &qpRemoteUserData.first;
        pRemoteDynamicRegionSizeBufferToken = &qpRemoteUserData.second;
        rdma_debug << "remote token1 is " << pRemoteRegionTokenBufferToken->getAddress() << ",local token1 is " << pLocalRegionTokenBufferToken->getAddress() << std::endl;

        // Create and register first local buffer.
        // Create another local buffer to indicate local buffer size, sothat remote know how many bytes to read.
    }

    static void bind(int listenPort) {
        qpFactory->bindToPort(listenPort);
    }
    void waitAndAccept() {
        // Factory method
        initFixedLocalBuffer();
        qpUserDataType qpUserData(*pLocalRegionTokenBufferToken, *pLocalDynamicRegionSizeBufferToken);
        pQP = qpFactory->acceptIncomingConnection(&qpUserData, sizeof(qpUserData));
        qpUserDataType qpRemoteUserData (*reinterpret_cast<qpUserDataType *>(pQP->getUserData()));
        pRemoteRegionTokenBufferToken = &qpRemoteUserData.first;
        pRemoteDynamicRegionSizeBufferToken = &qpRemoteUserData.second;
        rdma_debug << "remote token1 is " << pRemoteRegionTokenBufferToken->getAddress() << ",local token1 is " << pLocalRegionTokenBufferToken->getAddress() << std::endl;
        
    }
};

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaInitGlobal
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaInitGlobal(JNIEnv *, jobject) {
    try {
        context = new core::Context();
        qpFactory = new queues::QueuePairFactory(context);
    }
    catch (std::exception &ex) {
        rdma_error << "Exception: " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaDestroyGlobal
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaDestroyGlobal(JNIEnv *, jobject) {
    delete context;
    delete qpFactory;
    return 0;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaConnect
 * Signature: (Ljava/lang/String;I)Lorg/apache/hadoop/hbase/ipc/RdmaNative/RdmaConnection;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaConnect(JNIEnv *env, jobject, jstring jServerAddr, jint jServerPort) {

#define REPORT_FATAL(msg) do { \
    std::cerr << "RdmaNative FATAL: " << msg << "Unable to pass error to Java. Have to abort..." << std::endl; \
    abort(); \
} while(0)

    // Deal with java...
    static const jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative/RdmaConnection");
    if(jConnCls == NULL) REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative/RdmaConnection.");
    jmethodID jConnClsInit = env->GetMethodID(jConnCls, "<init>", "()V"); // -> problem!
    if (jConnClsInit == NULL) REPORT_FATAL("Unable to find constructor org/apache/hadoop/hbase/ipc/RdmaNative/RdmaConnection::<init> -> ()V.");
    jobject jConn = env->NewObject(jConnCls, jConnClsInit);
    if(jConn == NULL) REPORT_FATAL("Unable to create RdmaConnection object.");
    jfieldID jErrCode = env->GetFieldID(jConnCls, "errorCode", "I");
    if(jErrCode == NULL) REPORT_FATAL("Unable to getFieldId `errorCode` of class RdmaConnection.");

#define REPORT_ERROR(code, msg) do { \
    env->SetIntField(jConn, jErrCode, code); \
    std::cerr << "RdmaNative ERROR: " << msg << "Returning error code to java..." << std::endl; \
} while(0)

    // Deal with rdma logic...
    jboolean isCopy;
    const char *serverAddr = env->GetStringUTFChars(jServerAddr, &isCopy);
    if(serverAddr == NULL) REPORT_ERROR(3, "GetStringUTFChars from jServerAddr error.");
    // do connect





    // cleanup
    if (isCopy == JNI_TRUE) {
        env->ReleaseStringUTFChars(jServerAddr, serverAddr);
    }


}



