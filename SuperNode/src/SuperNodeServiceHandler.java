import java.util.*;
import node.*;
import supernode.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import java.lang.Math;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
public class SuperNodeServiceHandler implements SuperNodeService.Iface {

  private int dhtSize; // size of the DHT
  private boolean workInProgress; // an indicator that shows whether the supernode
                               // is in the process of creating a node  
  private int index; // keeps track of the index position of the nodeList
  private int m; // m bits
  Queue<String> waitingQueue;
  HashMap<Integer, String> joinedNodes;
  List<Integer> joinedNodesId;
  int counter = 0;
  int keyspace = 0;
  public SuperNodeServiceHandler(int dhtSize, int m) {

    this.dhtSize = dhtSize;
    this.workInProgress = false;
    this.index = 0;
    this.m = m;
    this.keyspace = (int) Math.pow(2, m);
    this.waitingQueue = new LinkedList<>();
    this.joinedNodes = new HashMap<>();
    this.joinedNodesId = new ArrayList<>();
  }

  public String getNodeForJoin(String ip, int port) {
    System.out.println(this.workInProgress);
    if (waitingQueue.size() > 0){
    	System.out.println("Get in waiting queue to join the system...");
      System.out.println("Unable to join >>> ip: " + ip + ", port: " + port);
      String info = ip + "," + port;
      waitingQueue.add(info);
      return "NACK";
    }
    //if waiting queue is empty but there's a node currently joining the system
    if (workInProgress) {
      System.out.println("Another node is joining...");
      System.out.println("Unable to join >>> ip: " + ip + ", port: " + port);
      String info = ip + "," + port;
      waitingQueue.add(info);
      return "NACK";
    }else{
      System.out.println("Started generating id for node");
    	workInProgress = true;
    	//first node to join, directly join the system, no need to join the queue
    	String info_joined_node = ip + "," + port;
    	String message = " ";
      System.out.println("ip address of new node is " + ip);
      int identifier = generateIdMD5(ip+port);
      while(joinedNodes.containsKey(identifier)){
          identifier = (identifier + 1)%keyspace;
      }
      System.out.println("generate identifier for node " + port + " is " + identifier);
    	if (this.joinedNodes.size() == 0){
    		this.joinedNodes.put(identifier, info_joined_node);
    		this.joinedNodesId.add(identifier);
    		System.out.println("First node to join");
    		message = identifier + ",none";
    		return message;
    	}else{
    		System.out.println("Not the first one");
    		int size = this.joinedNodes.size();
    		int index = (int)(Math.random()*size);
    		int id = joinedNodesId.get(index);
    		String info_contact_node = joinedNodes.get(id);
    		message = identifier + "," + info_contact_node + "," + id;
    		this.joinedNodes.put(identifier, info_joined_node);
    		this.joinedNodesId.add(identifier);
    		return message;
    	}
    	
    }
    
  }
  //function is invoked after a node is done joining the system
  public void postJoin(){
    System.out.println("Node joined successfully");
    this.workInProgress = false;
    String nextNode = waitingQueue.poll();
    //Continue to process the next node that want to join the system
    if (nextNode != null){
    	System.out.println(nextNode);
    	//indicate that there's a node joining in
    	this.workInProgress = true;
    	String[] info = nextNode.split(",");
    	String host = info[0];
    	int port = Integer.parseInt(info[1]);
      try{
        TTransport connection =  new TSocket(host,port);
        connection.open();
        TProtocol protocol = new TBinaryProtocol(connection);
        NodeService.Client client_stub = new NodeService.Client(protocol);
        int size = this.joinedNodes.size();
        int index = (int)(Math.random()*size);
        int id = joinedNodesId.get(index);
        String selected_node = joinedNodes.get(id);
        int our_id = generateIdMD5(host+port);
        //handle collision by gradually incrementing value of id
        while(joinedNodes.containsKey(our_id)){
            our_id++;
        }
        //return message for the node that wants to join
        String message = our_id + "," + selected_node + "," + id;
        joinedNodesId.add(our_id);
        joinedNodes.put(our_id,nextNode);
        counter++;
        //invoking the node to join the system
        client_stub.JoinSystem(message);
      }catch (TException e){
        e.printStackTrace();
        System.out.println("Failed to announce next node to join system");
      }
    }
    System.out.println("No nodes left in the waiting queue");
    //node finished joining the system and broadcasting message to other node
  }
  //generate id for node using normal hash function
  public int generateId(String hostname, int port){
    int identifier = 0;
    int l = hostname.length();
    for (int i = 0; i < l; i++){
      identifier = identifier + hostname.charAt(i) - 65;
    }
    identifier = identifier + port;
    identifier = identifier%this.keyspace;
    return identifier;
  }
  //generate id for nodes using MD5 hash function
  public int generateIdMD5(String key){
    try{
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(key.getBytes());
      byte[] digest = md.digest();
      int id = 0;
      //only take lower 16 bits
      for (int i = 0; i < 2; i++){
        id = id*256 + Math.abs(digest[i]);
      }
      return id%keyspace;
    }catch (NoSuchAlgorithmException error){
      return 0;
    }
  }
  //return contact node for client
  public String getNodeForClient() {
    int size = this.joinedNodes.size();
    int index = (int)(Math.random()*size);
    int id = joinedNodesId.get(index);
    String info_contact_node = joinedNodes.get(id);
    return info_contact_node;
  }
  //display finger table for all nodes in the system
  public String displayFingerTable(){
    int numnodes = joinedNodesId.size();
    String buffer = "";
    for (int i = 0; i < numnodes; i++){
      int id = joinedNodesId.get(i);
      String info = joinedNodes.get(id);
      String[] tokens = info.split(",");
      String host = tokens[0];
      int port = Integer.parseInt(tokens[1]);
      try{
        TTransport connection =  new TSocket(host,port);
        connection.open();
        TProtocol protocol = new TBinaryProtocol(connection);
        NodeService.Client client_stub = new NodeService.Client(protocol);
        try{
          String display_table = client_stub.DisplayFingerTable();
          buffer = buffer + "Finger table for host " + host + " with id " + id + "\n";
          buffer = buffer + display_table;
          buffer = buffer + "\n";
        }catch(TException e){
          System.out.println("Failed to invoke function");
        }
        connection.close();
      }catch (TTransportException e){
        System.out.println("fail to connect with node");
        e.printStackTrace();
      }
      
    }
    return buffer;

  }



}
