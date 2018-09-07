
package test.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;

public class Server {

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
    // buffer_size=1000;

    public native void stopRdma();

    static {
        System.loadLibrary("rdmaserverlib");
    }

    public void main(String[] args) {
        String hostname = "11.11.0.111";
        int port = 2333;
        Server server = null;
        try {
            connect();
            run();

        } catch (Exception e) {
            // TODO: handle exception
        } finally {
            // if (server != null) {
            disconnect();
            // }
        }

    }

    public void run() throws IOException {
        int contextId = initcontext();
        int qpFactoryId = initConnection(contextId);
        int bufferId = create2sideBuffer(contextId, 1000);
        ByteBuffer buffer = serverBuffer(bufferId);

        int tokenId = initBufferToken(bufferId);
        waitForConn(2333, qpFactoryId, tokenId);
        PollingForMes(contextId);

    }

    public void disconnect() {
        stopRdma();
    }

    public void connect() throws IOException {
    }
}