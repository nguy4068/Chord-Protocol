import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import node.*;
import java.util.Scanner;
import java.io.*;
import java.net.*;
/**
Class to listen for connection to node
 */
public class NodeServer{
	public static NodeServiceHandler nodeHandler;
	public static NodeService.Processor processor;
	public static int our_port = 4068;
	public static String our_host = "localhost";
	public static String super_node_host = "localhost";
	public static int super_node_port = 4068;
	public static void main(String[] args){
		try {
			//reads in configuration of its own and the super node config
			if (args.length == 3){
				//Read in super node config and current node configs
				String current_machine_config_file = args[0];
				String super_node_config_file = args[1];
				File current_machine_config = new File(current_machine_config_file);
				File super_node_config = new File(super_node_config_file);
				try{
					Scanner scanner = new Scanner(current_machine_config);
					String info = scanner.nextLine();
					String[] tokens = info.split(" ");
					our_host = tokens[0];
					our_port = Integer.parseInt(tokens[1]);
					scanner = new Scanner(super_node_config);
					String info_supernode = scanner.nextLine();
					tokens = info_supernode.split(" ");
					super_node_host = tokens[1];
					super_node_port = Integer.parseInt(tokens[2]);
				}catch (FileNotFoundException e){
					System.out.println("File not found");
				}

			}
			nodeHandler = new NodeServiceHandler(our_host, our_port, super_node_host, super_node_port);
			processor = new NodeService.Processor(nodeHandler);

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
	//starts listening for RPC
	public static void simple(NodeService.Processor processor){
		try{
			TServerTransport serverTransport = new TServerSocket(our_port);
			TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
			//System.out.println("Starting a multithreaded compute node server...");
			server.serve();
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
