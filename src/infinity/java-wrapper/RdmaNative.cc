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

class CRdmaConnectionInfo {
private:
    /* 4ptr is maintained by C++ class.
    private long ptrQP;
    private long ptrRegionTokenBuf; // registered as fixed length while making conn.
    private long ptrRemoteSerialBuf; // remote buffer serial number.
    private long ptrDynamicDataBuf; // must register on every local write. Invalidate it only if ptrRegionTokenBuf has changed!
    */
    queues::QueuePair *pQP; // must delete

    memory::Buffer *pLocalRegionTokenBuffer; // must delete
    memory::RegionToken *pLocalRegionTokenBufferToken; // must delete
    memory::RegionToken *pRemoteRegionTokenBufferToken; // must NOT delete

    void initRegionTokenBuffer() {
		pLocalRegionTokenBuffer = new infinity::memory::Buffer(context, sizeof(infinity::memory::RegionToken));
		pLocalRegionTokenBufferToken = pLocalRegionTokenBuffer->createRegionToken();
    }
public:
    CRdmaConnectionInfo() : pQP(nullptr), pLocalRegionTokenBuffer(nullptr), pLocalRegionTokenBufferToken(nullptr),
        pRemoteRegionTokenBufferToken(nullptr) {}
    ~CRdmaConnectionInfo() {
        if(pQP) delete pQP;
        if(pLocalRegionTokenBuffer) delete pLocalRegionTokenBuffer;
        if(pLocalRegionTokenBufferToken) delete pLocalRegionTokenBufferToken;
        if(pRemoteRegionTokenBufferToken) /*DO NOT DELETE IT!!!!!!!*/;
    }

    void connectToRemote(const char *serverAddr, int serverPort) {
        initRegionTokenBuffer();
        pQP = qpFactory->connectToRemoteHost(serverAddr, serverPort, pLocalRegionTokenBufferToken, sizeof(memory::RegionToken));
        pRemoteRegionTokenBufferToken = reinterpret_cast<memory::RegionToken *>(pQP->getUserData());
        // Create and register first local buffer.
        // Create another local buffer to indicate local buffer size, sothat remote know how many bytes to read.
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
        std::cerr << "RdmaNative: Exception: " << ex.what() << std::endl;
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


