#include "org_apache_hadoop_hbase_ipc_RdmaNative.h"
#include <iostream>

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaIsOpen
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaIsOpen(JNIEnv *, jobject, jobject) {

}

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaConnect
 * Signature: (ILjava/lang/String;)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaConnect(JNIEnv *, jobject, jint,
                                                        jstring);

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaBlockedAccept
 * Signature: (I)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL
Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaBlockedAccept(JNIEnv *, jobject,
                                                              jint);

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaClose
 * Signature: (Ljava/lang/Object;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaClose(
    JNIEnv *, jobject, jobject);

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaRead
 * Signature: (Ljava/lang/Object;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaRead(
    JNIEnv *, jobject, jobject);

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaReadable
 * Signature: (Ljava/lang/Object;)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaReadable(JNIEnv *, jobject,
                                                         jobject);

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaWrite
 * Signature: (Ljava/lang/Object;Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaWrite(JNIEnv *, jobject,
                                                      jobject, jobject);

/*
 * Class:     org_apache_hadoop_hbase_ipc_RdmaNative
 * Method:    rdmaRespond
 * Signature: (Ljava/lang/Object;Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL
Java_org_apache_hadoop_hbase_ipc_RdmaNative_rdmaRespond(JNIEnv *, jobject,
                                                        jobject, jobject);
