
package test.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;

public class Client {

    // client part
    public native int clientConnect(String ip, int port, int contextId);

    public native int clientInitRegionToken(int qpId);

    public native int initBuffer(int contextId);

    public native ByteBuffer clientBuffer(int bufferId);

    public native void rdmaWrite(int localBufferId, int remoteBufferTokenId, int contextId, int qpId);

    public native void rdmaRead(int localBufferId, int remoteBufferTokenId, int contextId, int qpId);

    public native void rdmaSend(int buffer2SidedId, int contextId, int qpId);

    // server part
    public native int initcontext();

    public native int initConnection(int contextId);

    public native int create2sideBuffer(int contextId, int buffer_size);

    public native ByteBuffer serverBuffer(int bufferId);

    public native int initBufferToken(int bufferId);

    public native void waitForConn(int port, int qpFactoryId, int bufferTokenId);

    public native int PollingForMes(int contextId);

    // commom

    public native void stopRdma();

    static {
        System.loadLibrary("rdmaclientlib");
    }

    public static void main(String[] args) {
        String hostname = "11.11.0.111";
        int port = 2333;
        Server server = null;
        try {

            connect();
            run();
        } catch (Exception e) {
            // GG
        } finally {
            // if (client != null) {
            disconnect();
            // }
        }

    }

    public void run() throws IOException {
        String hostname = "11.11.0.111";
        int port = 2333;
        int contextId = initcontext();
        int qpId = clientConnect(hostname, port, contextId);

        int tokenId = clientInitRegionToken(qpId);
        ByteBuffer test = initBuffer(contextId);
        rdmaWrite(test, tokenId, contextId, qpId);

        rdmaRead(localBufferId, tokenId, contextId, qpId);
        rdmaSend(buffer2SidedId, contextId, qpId);
    }

    public void disconnect() {
        stopRdma();
    }

    public void connect() throws IOException {

    }
}