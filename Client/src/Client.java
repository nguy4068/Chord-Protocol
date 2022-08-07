import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import java.io.*;
import java.util.*;
import supernode.*;
import node.*;
/**
Main class the user should interact with the system from
 */
public class Client {
	public static HashMap<String,String> dictionary;
	public static void main(String[] args){
    		String supernodeIp = "kh4250-01.cselabs.umn.edu";
    		int supernodePort = 4068;;
		//read in server address from machine.txt file
		//get the dictionary file and distribute the words to nodes
		dictionary = new HashMap<>();
      		if (args.length == 2) {

        		// read the file and put the defs and meanings into the DHT
        		String machine_config_file = args[0];
        		String dictionary_file = args[1];
        		File machine_file = new File(machine_config_file);
        		try{
        			//scanning in supernode information
        			Scanner s = new Scanner(machine_file);
        			System.out.println("Scanning in super node information");
        			while (s.hasNext()){	
        				String info = s.nextLine();
        				String[] tokens = info.split(" ");
        				if (tokens[0].equals("supernode")){
        					System.out.println("Find supernode config");
        					supernodeIp = tokens[1];
        					supernodePort = Integer.parseInt(tokens[2]);
        					break;
        				}
        			}
					//start client work
        			startClient(supernodeIp, supernodePort, dictionary_file);
        			
        		}catch (FileNotFoundException e){
        			System.out.println("Wrong file path");
        		}

      		} else {
        		System.out.println("Please provide a dictionary file");
      		}


  }	
  public static void startClient(String spHost, int spPort, String dictionary_file_path){
  	try{
		//build the dictionary and listen to input
  		ConstructDictionary(spHost, spPort, dictionary_file_path);
  	}catch (TTransportException e){
  		e.printStackTrace();
  	}
  }

  // A helper function to enter all the pairs from a given file 
  // to initialize the DHT
  public static void ConstructDictionary(String spHost, int spPort, String file_path) throws TTransportException {
    File file = new File(file_path);
    Scanner scanner;
    try {
	  //set up connection with super node
      scanner = new Scanner(file);
      System.out.println("Connect with super node");
      TTransport connection =  new TSocket(spHost,spPort);
      connection.open();
      TProtocol protocol = new TBinaryProtocol(connection);
      SuperNodeService.Client supernode_stub = new SuperNodeService.Client(protocol);
      System.out.println("Connect with supernode successfully");
      System.out.println("Try to get contact node from super node");
      try{
		    //set up the connection with contact node
      		String contact_node = supernode_stub.getNodeForClient();
      		String[] tokens = contact_node.split(",");
      		String contact_ip = tokens[0];
      		int contact_port = Integer.parseInt(tokens[1]);
      		connection = new TSocket(contact_ip, contact_port);
      		connection.open();
      		protocol = new TBinaryProtocol(connection);
      		NodeService.Client node_stub = new NodeService.Client(protocol);
      		System.out.println("Finished setting up connection with node");
      		System.out.println("Started to read in dictionary file path, please wait");
      		try{
				//started to interact with user to take in input
      			while (scanner.hasNext()) {
      				String word = scanner.nextLine();
      				String definition = scanner.nextLine();
      				dictionary.put(word,definition);
      				boolean result = node_stub.put(word,definition,false);
      				if (result){
      					System.out.println("Successfully put words");
      				}
      			}
				
      			System.out.println("Finished scanning through the dictionary");
      			System.out.println("Please use the following command to add more words to dictionary or get a definition of a word");
      			System.out.println("1.Add a word: put [word] [definition]");
      			System.out.println("2.Get a word: get [word]");
      			System.out.println("3.Quit: quit");
			System.out.println("3.Show all finger tables of nodes: fingertable");
      			scanner = new Scanner(System.in);
				//while scanner keeps inputting
      			while (scanner.hasNext()){
      				String input = scanner.nextLine();
      				if (input.equals("quit")){
      					connection.close();
      					System.out.println("good bye");
      					break;
					}else if(input.equals("fingertable")){
						System.out.println("Display finger tables for all nodes\n");
						String output = supernode_stub.displayFingerTable();
						System.out.println(output);
						continue;
					}
      				String[] commands = input.split(" ");
      				if (commands.length == 2){
      					if (commands[0].equals("get")){
      						String definition = node_stub.get(commands[1], false);
							System.out.println(definition);
      					}else{
      						System.out.println("wrong command");
      					}
						
      					
      				}else if (commands.length == 3){
      					if (commands[0].equals("put")){
						System.out.println("Starting put words");
      						boolean r = node_stub.put(commands[1], commands[2], false);
				System.out.println(r);		
						if (r){
							System.out.println("Successfully put words");
						}
      					}else{
      						System.out.println("Wrong commands");
      					}
      				}else{
						if (commands[0].equals("put")){
							String recombine = "";
							for (int i = 2; i < commands.length; i++){
								recombine = recombine + " " + commands[i];
							}
							boolean r = node_stub.put(commands[1], recombine, false);
							if (r){
								System.out.println("Finished adding words");
							}
						}else{
      						System.out.println("Wrong commands");
						}
      				}
      			}
      			
      			
      		}catch (TException e){
      			System.out.println("Failed to contact node");
      			e.printStackTrace();
      		}	
      }catch (TException e){
      		e.printStackTrace();
      		System.out.println("Failed to contact super node");
      		return;
      }
      
    } catch (FileNotFoundException e) {
      System.out.println("dictionary file not found");
    }

   
  }

  private static String getNode(String supernodeIp, int supernodePort) throws TTransportException{
      // TODO: should I catch exception
      TTransport connection = new TSocket(supernodeIp, supernodePort);
      connection.open();
      TProtocol protocol = new TBinaryProtocol(connection);
      SuperNodeService.Client client = new SuperNodeService.Client(protocol);
      try{
      	String node = client.getNodeForClient();
      	connection.close();
      	return node;
      }catch (TException e){
      	e.printStackTrace();
      }
      return "none";
      
  }

  private static void put(String nodeInfo, String word, String meaning) throws TException {
      // connect to a node

      System.out.println("Putting word: " + word + ", meaning: " + meaning);
      // TODO: get the node address from string
      String[] tokens = nodeInfo.split(",");
      String nodeIP = tokens[0];
      int nodePort = Integer.parseInt(tokens[1]);
      TTransport connection = new TSocket(nodeIP, nodePort);
      connection.open();
      TProtocol protocol = new TBinaryProtocol(connection);
      NodeService.Client client = new NodeService.Client(protocol);
      client.put(word, meaning, false);
      connection.close();
  }

  private static void get(String nodeInfo, String word) throws TException {
      // connection to a node
      // TODO: get the node address from string
      String[] tokens = nodeInfo.split(",");
      String nodeIP = tokens[0];
      int nodePort = Integer.parseInt(tokens[1]);
      TTransport connection = new TSocket(nodeIP, nodePort);
      connection.open();
      TProtocol protocol = new TBinaryProtocol(connection);
      NodeService.Client client = new NodeService.Client(protocol);
      String result = client.get(word, false);

      System.out.println("result: " + result);
      
      connection.close();
  }
	
}
