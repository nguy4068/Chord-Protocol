import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import supernode.*;
import node.*;
import java.util.*;
import java.io.*;
/**
Set up super node multithreaded server
 */
public class SuperNodeServer{
	public static SuperNodeServiceHandler superNodeHandler;
	public static SuperNodeService.Processor processor;
	public static int DHTSize = 0;
	public static int m = 12;
	public static String host = "localhost";
	public static int port = 4068;
	public static void main(String[] args){
		try {
			if (args.length == 2){
				DHTSize = Integer.parseInt(args[0]);
				m = Integer.parseInt(args[1]);
				
			}
			superNodeHandler = new SuperNodeServiceHandler(DHTSize, m);
			processor = new SuperNodeService.Processor(superNodeHandler);

		Runnable simple = new Runnable() {
			public void run(){
				simple(processor);
			}
		};
		new Thread(simple).start();
	}catch (Exception x){
		x.printStackTrace();
	}
	}
	public static void simple(SuperNodeService.Processor processor){
		try{
			TServerTransport serverTransport = new TServerSocket(port);
			TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
			//System.out.println("Starting a multithreaded compute node server...");
			server.serve();
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
