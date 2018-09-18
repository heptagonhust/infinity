#include "org_apache_hadoop_hbase_ipc_RdmaNative.h"
#include "org_apache_hadoop_hbase_ipc_RdmaNative_RdmaClientConnection.h"
#include "org_apache_hadoop_hbase_ipc_RdmaNative_RdmaServerConnection.h"

#include "RdmaImpl.hpp"

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
    checkedDelete(qpFactory);
    checkedDelete(context);
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaConnect
 * Signature: (Ljava/lang/String;I)Lorg/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaConnect(JNIEnv *env, jobject self, jstring jServerAddr,
                                                                                  jint jServerPort) {
#define REPORT_FATAL(msg)                                                                                                      \
    do {                                                                                                                       \
        std::cerr << "RdmaNative FATAL: " __FILE__ ":" << __LINE__ << msg << "Unable to pass error to Java. Have to abort..." << std::endl;             \
        abort();                                                                                                               \
    } while (0)

    rdma_debug << "rdmaConnect called, server port " << jServerPort << std::endl;

    jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection.");
    jmethodID jConnClsInit = env->GetMethodID(jConnCls, "<init>", "(Lorg/apache/hadoop/hbase/ipc/RdmaNative;)V");
    if (jConnClsInit == NULL)
        REPORT_FATAL("Unable to find constructor org/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection::<init> -> (org/apache/hadoop/hbase/ipc/RdmaNative;)V.");
    jobject jConn = env->NewObject(jConnCls, jConnClsInit, self);
    if (jConn == NULL)
        REPORT_FATAL("Unable to create RdmaClientConnection object.");

#define REPORT_ERROR(code, msg)                                                                                                \
    do {                                                                                                                       \
        std::cerr << "RdmaNative ERROR: " __FILE__ ":" << __LINE__ << msg << "Returning error code to java..." << std::endl;                            \
        return NULL;
    } while (0)

    jboolean isCopy;
    const char *serverAddr = env->GetStringUTFChars(jServerAddr, &isCopy);
    if (serverAddr == NULL)
        REPORT_ERROR(3, "GetStringUTFChars from jServerAddr error.");

    rdma_debug << "server addr:" << serverAddr << std::endl;

    // do connect
    try {
        CRdmaClientConnectionInfo *pConn = new CRdmaClientConnectionInfo();
        pConn->connectToRemote(serverAddr, jServerPort);
        jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
        if (jFieldCxxPtr == NULL)
            REPORT_ERROR(5, "Unable to getFieldId `ptrCxxClass`");
        static_assert(sizeof(jlong) == sizeof(CRdmaClientConnectionInfo *), "jlong must have same size with C++ Pointer");
        env->SetLongField(jConn, jFieldCxxPtr, (jlong)pConn);
    } catch (std::exception &e) {
        REPORT_ERROR(4, e.what());
    }

    // cleanup
    if (isCopy == JNI_TRUE) {
        env->ReleaseStringUTFChars(jServerAddr, serverAddr);
    }
    rdma_debug << "connected! jConn is " << jConn << std::endl;
    return jConn;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaBind
 * Signature: (I)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaBind(JNIEnv *, jobject, jint jListenPort) {
    rdma_debug << "Binding port " << jListenPort << std::endl;
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
 * Signature: (I)Lorg/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaBlockedAccept(JNIEnv *env, jobject self) {
    rdma_debug << "accepting..." << std::endl;
    jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection.");
    jmethodID jConnClsInit = env->GetMethodID(jConnCls, "<init>", "(Lorg/apache/hadoop/hbase/ipc/RdmaNative;)V");
    if (jConnClsInit == NULL)
        REPORT_FATAL("Unable to find constructor org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection::<init> -> (org/apache/hadoop/hbase/ipc/RdmaNative;)V.");
    jobject jConn = env->NewObject(jConnCls, jConnClsInit, self);
    if (jConn == NULL)
        REPORT_FATAL("Unable to create RdmaServerConnection object.");

    try {
        CRdmaServerConnectionInfo *pConn = new CRdmaServerConnectionInfo();
        pConn->waitAndAccept();
        jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
        if (jFieldCxxPtr == NULL)
            REPORT_ERROR(5, "Unable to getFieldId `ptrCxxClass`");
        static_assert(sizeof(jlong) == sizeof(CRdmaClientConnectionInfo *), "jlong must have same size with C++ Pointer");
        env->SetLongField(jConn, jFieldCxxPtr, (jlong)pConn);
    } catch (std::exception &e) {
        REPORT_ERROR(4, e.what());
        return NULL;
    }
    rdma_debug << "Accepted! jconn is " << jConn << std::endl;
    return jConn;
}

////////////////////////////////////// RdmaClientConnection Methods
#undef REPORT_ERROR
#define REPORT_ERROR(msg)                                                                                                      \
    do {                                                                                                                       \
        std::cerr << "RdmaNative ERROR: " __FILE__ ":" << __LINE__  << msg << "Returning error code to java..." << std::endl;                            \
        return JNI_FALSE;                                                                                                      \
    } while (0)
#define REPORT_ERROR_B(msg)                                                                                                    \
    do {                                                                                                                       \
        std::cerr << "RdmaNative ERROR: " __FILE__ ":" << __LINE__  << msg << "Returning error code to java..." << std::endl;                            \
        return NULL;                                                                                                           \
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
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaClientConnection_readResponse(JNIEnv *env,
                                                                                                             jobject self) {
    jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_ERROR_B("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaClientConnectionInfo *pConn = (CRdmaClientConnectionInfo *)cxxPtr;
    if (pConn == nullptr)
        REPORT_ERROR_B("cxx conn ptr is nullptr. is the connection closed?");

    checkedDelete(previousResponseDataPtr);
    try {
        pConn->readResponse(previousResponseDataPtr);
        if (previousResponseDataPtr == nullptr)
            throw std::runtime_error("readResponse return null");
    } catch (std::exception &e) {
        REPORT_ERROR_B(e.what());
    }
    return env->NewDirectByteBuffer(previousResponseDataPtr->getData(), previousResponseDataPtr->getSizeInBytes());
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaClientConnection
 * Method:    writeQuery
 * Signature: (Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaClientConnection_writeQuery(
    JNIEnv *env, jobject self, jobject jDataBuffer) {
    jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_ERROR("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaClientConnectionInfo *pConn = (CRdmaClientConnectionInfo *)cxxPtr;
    if (pConn == nullptr)
        REPORT_ERROR("cxx conn ptr is nullptr. is the connection closed?");

    void *tmpJAddr = env->GetDirectBufferAddress(jDataBuffer);
    if (tmpJAddr == NULL)
        REPORT_ERROR("jDataBuffer addr is null");
    try {
        pConn->writeQuery(tmpJAddr, env->GetDirectBufferCapacity(jDataBuffer));
    } catch (std::exception &e) {
        REPORT_ERROR(e.what());
    }

    return JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaClientConnection
 * Method:    close
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaClientConnection_close(JNIEnv *env,
                                                                                                       jobject self) {
    jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative$RdmaClientConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_ERROR("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaClientConnectionInfo *pConn = (CRdmaClientConnectionInfo *)cxxPtr;
    if (pConn == nullptr)
        REPORT_ERROR("cxx conn ptr is nullptr. is the connection closed?");

    checkedDelete(pConn);
    return JNI_TRUE;
}

/////////////////////////////////////////////////////////RdmaServerArea
/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaServerConnection
 * Method:    isClosed
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaServerConnection_isClosed(JNIEnv *, jobject) {
    return JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaServerConnection
 * Method:    isQueryReadable
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaServerConnection_isQueryReadable(JNIEnv *env,
                                                                                                                 jobject self) {
    rdma_debug << "isQueryReadable called with self=" << self << std::endl;
    jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_FATAL("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaServerConnectionInfo *pConn = (CRdmaServerConnectionInfo *)cxxPtr;
    if (pConn == nullptr)
        REPORT_FATAL("cxx conn ptr is nullptr. is the connection closed?");

    try {
        return pConn->isQueryReadable();
    } catch (std::exception &e) {
        REPORT_FATAL(e.what());
    }
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaServerConnection
 * Method:    readQuery
 * Signature: ()Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaServerConnection_readQuery(JNIEnv *env,
                                                                                                          jobject self) {
    jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_ERROR_B("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaServerConnectionInfo *pConn = (CRdmaServerConnectionInfo *)cxxPtr;
    if (pConn == nullptr)
        REPORT_ERROR_B("cxx conn ptr is nullptr. is the connection closed?");

    try {
        void *dat = nullptr;
        uint64_t size = 0;
        pConn->readQuery(dat, size);
        return env->NewDirectByteBuffer(dat, size);
    } catch (std::exception &e) {
        REPORT_ERROR_B(e.what());
    }
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaServerConnection
 * Method:    writeResponse
 * Signature: (Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaServerConnection_writeResponse(
    JNIEnv *env, jobject self, jobject jDataBuffer) {
    jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_ERROR("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaServerConnectionInfo *pConn = (CRdmaServerConnectionInfo *)cxxPtr;
    if (pConn == nullptr)
        REPORT_ERROR("cxx conn ptr is nullptr. is the connection closed?");

    try {
        void *tmpDataBuf = env->GetDirectBufferAddress(jDataBuffer);
        if (tmpDataBuf == nullptr)
            REPORT_ERROR("writeresponse jDataBuffer addr is null");
        pConn->writeResponse(tmpDataBuf, env->GetDirectBufferCapacity(jDataBuffer));
    } catch (std::exception &e) {
        REPORT_ERROR(e.what());
    }
    return JNI_TRUE;
}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative_RdmaServerConnection
 * Method:    close
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_00024RdmaServerConnection_close(
    JNIEnv *env, jobject self) {
    jclass jConnCls = env->FindClass("org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection");
    if (jConnCls == NULL)
        REPORT_FATAL("Unable to find class org/apache/hadoop/hbase/ipc/RdmaNative$RdmaServerConnection.");
    jfieldID jFieldCxxPtr = env->GetFieldID(jConnCls, "ptrCxxClass", "J");
    if (jFieldCxxPtr == NULL)
        REPORT_ERROR("Unable to getFieldId `ptrCxxClass`");
    jlong cxxPtr = env->GetLongField(self, jFieldCxxPtr);
    CRdmaServerConnectionInfo *pConn = (CRdmaServerConnectionInfo *)cxxPtr;
    if (pConn == nullptr)
        REPORT_ERROR("cxx conn ptr is nullptr. is the connection closed?");

    checkedDelete(pConn);
    return JNI_TRUE;
}
