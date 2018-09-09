package org.apache.hadoop.hbase.ipc;

import java.nio.ByteBuffer;
public class RdmaNative {
    public native boolean rdmaIsOpen(Object RdmaConnection);
    public native Object rdmaConnect(int port,String addr);
    public native Object rdmaBlockedAccept(int port);
    public native int rdmaClose(Object RdmaConnection);
    public native ByteBuffer rdmaRead(Object RdmaConnection);
    public native boolean rdmaReadable(Object RdmaConnection);
    public native boolean rdmaWrite(Object RdmaConnection, ByteBuffer sbuf);
    public native boolean rdmaRespond(Object RdmaConnection, ByteBuffer sbuf);

    public class RdmaConnection {
        private long ptrQP;
    }
}
