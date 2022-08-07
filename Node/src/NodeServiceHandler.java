import node.*;
import supernode.*;
import java.util.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import java.lang.Math;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
/*
Node class will be a decorator to helps node normally handle RPC request
*/
class Node{
	NodeService.Client client_stub;
	TTransport connection;
	String host;
	int port;
	int id;
	boolean ishost = false;
	int keyspace;
	int m = 12;
	Node[] finger_table;
	HashMap<String,String> local_storage;
	Node contact_node;
	Node predecessor;
	HashMap<String,String> cache;
	//Constructor takes in node information
	public Node(int id, String host, int port, boolean ishost){
		this.host = host;
		this.id = id;
		this.port = port;
		this.ishost = ishost;
		this.keyspace = (int) Math.pow(2,m);
		this.local_storage = new HashMap<>();
		this.cache = new HashMap<>();
	}
	//JoinSystem is invoked when the node can join the system immediately
	//or after the previous node has finished joining the system, in this case
	//this function will be invoked from the supernode
	public void JoinSystem(String message){
		String[] tokens = message.split(",");
		//if it's not the first node to join the system
		if (!tokens[1].equals("none")){
			String target_node_host = tokens[1];
			int target_node_port = Integer.parseInt(tokens[2]);
			int target_node_id = Integer.parseInt(tokens[3]);
			this.contact_node = new Node(target_node_id, target_node_host, target_node_port, false);
			this.InitFingerTable();
			this.UpdateOthers();
		}else{
			//if it's the first node to join the system
			System.out.println("first node to join");
			this.finger_table = new Node[m];
			this.contact_node = null;
			this.InitFingerTable();
			
		}
	}
	//Function used to find predecessor of an id (node), or a key (word)
	String FindPredecessor(int id){
		//if the node does not run on the current host, then we'll need to make RPC call
		if (!ishost){
			try {
				TTransport connection =  new TSocket(this.host,this.port);
				connection.open();
				TProtocol protocol = new TBinaryProtocol(connection);
				NodeService.Client client_stub = new NodeService.Client(protocol);
				String predecessor = client_stub.FindPredecessor(id);
				connection.close();
				return predecessor;
			} catch (TException e){
				System.out.println("Failed to get predecessor");
				return "none";
			}
		}else{
			//if the node is running on current host then we directly handle the logic here
			System.out.println("Find predecessor for node " + id); 
			int lower_bound = this.id;
			int upper_bound = 0;
			if (this.finger_table[0] == null){
				System.out.println("Sucessor of node " + this.id + " is null");
			
			}else{
				System.out.println("Successor not null");
				upper_bound = this.finger_table[0].id;
			}
			//Initialize the target node to our current node
			Node current = this;
			boolean found = false;
			//while the predecessor has not been found
			while (!found){
				boolean condition = false;
				//check if the id is between current and current.successor
				System.out.println("lower bound " + lower_bound + " upper bound " + upper_bound + " id " + id); 
				if (lower_bound >= upper_bound){
					condition = ((id > lower_bound) || (id <= upper_bound));
				}else{
					condition = ((id > lower_bound) && (id <= upper_bound));
				}
				if (condition){
				//found the predecessor
			    	return current.id + "," + current.host + "," + current.port;
				}

				//if the current node is node the predecessor, then check other nodes
				//inside finger table that might be closest to the current id
				System.out.println("Find closest preceding finger");
				String next_node = current.ClosestPrecedingFinger(id);
				String[] tokens = next_node.split(",");
				int next_node_id = Integer.parseInt(tokens[0]);
				//if the next node is not our current node, then
				//we need to make RPC to its function, instead of
				//simply invoking function from the handler
				//if the result of previous search is similar to this search, then break to avoid infinite loop
				if (next_node_id == current.id){
					System.out.println("Current node does not change");
					break;
				}
				if (next_node_id != this.id){
					current = new Node(next_node_id, tokens[1], Integer.parseInt(tokens[2]), false);
				}else{
					current = this;
				}
				//update lower bound upper bound
				lower_bound = current.id;
				String successor = current.GetSuccessor();
				tokens = successor.split(",");
				upper_bound = Integer.parseInt(tokens[0]);
							
			
			}
			return current.id + "," + current.host + "," + current.port;

		}
	}
	/**
	Function used to find successor of an id or a node
	 */
	String FindSuccessor(int target_id){
		//if the node does not run on the current host, then we'll need to make RPC call
		if (!ishost){
			try {
				TTransport connection =  new TSocket(this.host,this.port);
				connection.open();
				TProtocol protocol = new TBinaryProtocol(connection);
				NodeService.Client client_stub = new NodeService.Client(protocol);
				String successor = client_stub.FindSuccessor(target_id);
				connection.close();
				return successor;
			}catch (TException e){
				e.printStackTrace();
				System.out.println("Failed to get successor");
				return "none";
			}
		}else{
		//First try to look for the successor of id from finger table
		int distance = target_id - this.id;
		double power = (Math.log(distance) / Math.log(2));
		int round = (int) power;
		//appears in the table
		System.out.println("Find successsor for node " + id);
		if (round == power && round < m){
			Node successor = this.finger_table[round];
			if (successor == null){
				return this.id + "," + this.host + "," + this.port;
			}else{
				return successor.id + "," + successor.host + "," + successor.port;
			}
		}else{
			//if successor does not appear in finger table, then we need to look up for predecessor first, and then
			//successor of the predcessor, by this way, we make sure if there exists node 'k', then successor of node 'k' must be 'k'
			String predecessor = this.FindPredecessor(target_id);
			Node node = GetNode(predecessor);
			return node.GetSuccessor();
		}
		}
	}
	/**
	Function used to find nodes that closely precedes the provided target_id
	 */
	String ClosestPrecedingFinger(int target_id){
		//if the node does not run on the current host, then we'll need to make RPC call
		if (!ishost){
			try{
				TTransport connection =  new TSocket(this.host,this.port);
				connection.open();
				TProtocol protocol = new TBinaryProtocol(connection);
				NodeService.Client client_stub = new NodeService.Client(protocol);
				String closest = client_stub.ClosestPrecedingFinger(target_id);
				connection.close();
				return closest;
			}catch (TException e){
				System.out.println("Failed to find closest preceding finger");
				return "none";
			}
		}else{
			//check from the furthest node from current node to reduce search time
			int lower_bound = this.id;
			int upper_bound = target_id;
			for (int i = m - 1; i >= 0; i--){
				int node_id = this.finger_table[i].id;
				//if the node is within the bound of current node -> target node, then it can be a predecessor candidates
				if (lower_bound >= upper_bound){
					if (node_id > lower_bound || node_id < upper_bound){
						return node_id + "," +  this.finger_table[i].host + "," + this.finger_table[i].port;
					}
				}else{
					if (lower_bound < node_id && node_id < upper_bound){
						return node_id + "," + this.finger_table[i].host + "," + this.finger_table[i].port;
					}
				}
			}
			//return current node if could not find any
			return this.id + "," + this.host + "," + this.port;
		}
	}
	/**
	Initialize finger table for a node when first joining system
	 */
	void InitFingerTable(){
		finger_table = new Node[m];
		if (contact_node == null){
			//first node to join the network
			System.out.println("First node to join the network");
			for (int i = 0; i < m; i++){
			        //initialize itself to every entry in finger table;
				this.finger_table[i] = this;
			}
			predecessor = this;

		}else{
			//first find the successor, and update the connection of 
			//the predecessor, successor of new node
			String successor_info = this.contact_node.FindSuccessor(this.id);
			Node successor = GetNode(successor_info);
			String predecessor_info = successor.GetPredecessor();
			this.finger_table[0] = successor;
			this.predecessor = GetNode(predecessor_info);
			this.predecessor.SetSuccessor(this.id + "," + this.host + "," + this.port);
			successor.SetPredecessor(this.id + "," + this.host + "," + this.port);
			//Find successor for each key
			for (int i = 1; i <= m - 1; i++){
				int start = this.id + (int) Math.pow(2,i);
				int prev_start = this.id + (int) Math.pow(2,i-1);
				int prev_successor = this.finger_table[i-1].id;
				start = start % keyspace;
				prev_start = prev_start % keyspace;
				boolean same = false;
				//use the old successor if the current key is still within the previous bound
				if (prev_start < prev_successor){
					if (prev_start <= start && start <= prev_successor){
						same = true;
					}
				}else if(prev_start > prev_successor){
					if (start >= prev_start || start <= prev_successor){
						same = true;
					}
				}
				if (same){
					this.finger_table[i] = this.finger_table[i-1];
				}
				else{
					//ask the contact node if it does not know
					successor_info = this.contact_node.FindSuccessor(start);
					this.finger_table[i] = GetNode(successor_info);
					
				}
				
			}
		}
	}
	/**
	Function to display finger table
	 */
	public String DisplayFingerTable(){
		String buffer = "";
		for (int i = 0; i < m; i++){
			int start = this.id + (int)Math.pow(2,i);
			start = start %keyspace;
			buffer = buffer + start + " " + this.finger_table[i].id + "\n";
			
		}
		return buffer;
	}
	//function to add words to the dictionary
	boolean put(String word, String meaning, boolean sure){
		System.out.println("node " + this.id + " received request to find word " + word);
		//this is to prevent the indefinitely loop of the system
		//if sure is true then we node our current node should be the
		//right node to put the word, if not then we search for the correct node
		if (cache.containsKey(word)){
			//also update the local cache
			cache.put(word, meaning);
		}
		if (!sure){
			Node correct_node = getCorrectNode(word);
			//if the correct node is a remote node then  make RPC
			if (!correct_node.ishost){
				System.out.println("word sure be resolved from remote noed " + correct_node.id);
				try{
					TTransport connection =  new TSocket(correct_node.host,correct_node.port);
					connection.open();
					TProtocol protocol = new TBinaryProtocol(connection);
					NodeService.Client client_stub = new NodeService.Client(protocol);
					client_stub.put(word,meaning, true);
					connection.close();
					return true;
				}catch (TException e){
					e.printStackTrace();
					return false;
				}
			}else{
				//if the correct node is the current node itself
				System.out.println("Word " + word + " can be resolved locally from node " + this.id);
				local_storage.put(word,meaning);
				int current_size = local_storage.size();
				System.out.println("node " + this.id + " is in charge of " +current_size + " words");
				return true;
			}

		}else{
			//if has been sure then directly put it in local storage without needing to search for predecessor anymore
			int current_size = local_storage.size();
			System.out.println("node " + this.id + " is in charge of " +current_size + " words");
			local_storage.put(word,meaning);
			return true;

		}
	}
	//Hashing a word using MD5 algorithm
	public int HashWord(String word){
		try{
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(word.getBytes());
			byte[] digest = md.digest();
			int id = 0;
			//only used the lowest 16 bits, and mod by keyspace (which is 2^12)
			for (int i = 0; i < 2; i++){
				id = id*2 + Math.abs(digest[i]);
			}
			int key = id%keyspace;	
			System.out.println("Hashing value for word " + word + " is " + key);
			return key;
		}catch (NoSuchAlgorithmException error){
			return 0;
		}
	}
	//Function to update DHT of a node at a specific index
	void UpdateDHT(String new_node, int index){
		if (!ishost){
			try{
				System.out.println(host + " " + port);
				TTransport connection =  new TSocket(this.host,this.port);
				connection.open();
				TProtocol protocol = new TBinaryProtocol(connection);
				NodeService.Client client_stub = new NodeService.Client(protocol);
				client_stub.UpdateDHT(new_node, index);
				connection.close();
			}catch(TException e){
				e.printStackTrace();
			}
		}else{
			int current_successor = this.finger_table[index].id;
			int distance = (int) Math.pow(2,index);
			int start = this.id + distance;
			start = start%keyspace;
			String[] tokens = new_node.split(",");
			int new_successor_id = Integer.parseInt(tokens[0]);
			if (current_successor < start){
				//interval pass starts of cycle
				//only fix the finger if the new successor id is
				//smaller than current successor id (or the new successor
				//is between the target node and its current successor
				//otherwise we still keep the original successor no need to change
				if (new_successor_id >= start || new_successor_id < current_successor){
					this.finger_table[index] = GetNode(new_node);
					//update other successor entries
					try{
						predecessor.UpdateDHT(new_node, index);
						//NL
						System.out.println("Fix finger node: " + this.id + " index " + index);
					}catch (Exception e){
						System.out.println("Socket has already been set up, the predecessor is the one that broadcast this update, no need to rebroadcast");

					}
				}
			}else{
			//normal interval
				if (new_successor_id >= start && new_successor_id < current_successor){
					this.finger_table[index] = GetNode(new_node);
					//update other entries
					try{
					predecessor.UpdateDHT(new_node, index);
					//NL
					System.out.println("Fix finger node: " + this.id + " index " + index);
					}catch (Exception e){
						System.out.println("Socket has already been set up, the predecessor is the one that broadcast this update, no need to rebroadcast");
					}
				}
			}
			
		}
	}
	/**
	Function that get the successor of a key
	 */
	Node getCorrectNode(String word){
		int key = HashWord(word);
			//correct node to resolve the word
		if (key == this.id){
			System.out.println("I am the correct node to resolve the word " + word + " with key " + key);
			return this;
		}else{
			String correct_node = FindSuccessor(key);
			Node node = GetNode(correct_node);
			System.out.println("correct node to find word " + word + " is " + node.id);
			return node;
		}

	}
	//Function that helps us get definition of a word
	String get(String word, boolean sure){
		System.out.println("node " + this.id + " received request to find word " + word);
		if (cache.containsKey(word)){
			return cache.get(word);
		}
		if (local_storage.containsKey(word)){
			return local_storage.get(word);
		}
		if (!sure){
			Node correct_node = getCorrectNode(word);
			if (!correct_node.ishost){
				try{
					TTransport connection =  new TSocket(correct_node.host,correct_node.port);
					connection.open();
					TProtocol protocol = new TBinaryProtocol(connection);
					NodeService.Client client_stub = new NodeService.Client(protocol);
					String r = client_stub.get(word, true);
					//save that word to local cache
					cache.put(word, r);
					connection.close();
					return r;
				}catch (TException e){
					e.printStackTrace();
					return "failed to find words";
				}
			}else{
				System.out.println("Word " + word + " can be resolved locally from node " + this.id);
				if (local_storage.containsKey(word)){
					System.out.println("I was able to find the word");
					return local_storage.get(word);
				}else{
					System.out.println("I was not able to find the word");
					return "can't find definition for words";
				}
			}

		}else{
			if (local_storage.containsKey(word)){
				System.out.println("I was able to find the word");
				return local_storage.get(word);
			}else{
				System.out.println("I was not able to find the word");
				return "can't find definition for words";
			}

		}
		
	}
	//Function that returns the successor of our current node, which is either the node itself
	//if system has only one node, or node it points to
	String GetSuccessor(){
		if (!ishost){
			try{
				System.out.println("Get successor for host " + this.host + " with id " + this.id);
				TTransport connection =  new TSocket(this.host,this.port);
				connection.open();
				TProtocol protocol = new TBinaryProtocol(connection);
				NodeService.Client client_stub = new NodeService.Client(protocol);
				String r =  client_stub.GetSuccessor();
				connection.close();
				System.out.println("Successor for node " + this.id + " is " + r);
				return r;
			}catch (TException e){
				e.printStackTrace();
				return "none";
			}
		}else{
			Node successor = this.finger_table[0];
			System.out.println("Successor for node " + this.id + " is " + successor.id);
			return successor.id + "," + successor.host + "," + successor.port;
		}
	}
	//returns predecessor of the current node, which is either the node itself if system has only one node, or the previous node that points to it
	String GetPredecessor(){
		if (!ishost){
			try{
				TTransport connection =  new TSocket(this.host,this.port);
				connection.open();
				TProtocol protocol = new TBinaryProtocol(connection);
				NodeService.Client client_stub = new NodeService.Client(protocol);
				String r =  client_stub.GetPredecessor();
				connection.close();
				return r;
			}catch (TException e){
				e.printStackTrace();
				return "none";
			}
		}else{
			return predecessor.id + "," + predecessor.host + "," + predecessor.port;
		}
	}
	//Set successor for current node
	void SetSuccessor(String successor_info){
		if (!ishost){
			try{
				TTransport connection =  new TSocket(this.host,this.port);
				connection.open();
				TProtocol protocol = new TBinaryProtocol(connection);
				NodeService.Client client_stub = new NodeService.Client(protocol);
				client_stub.SetSuccessor(successor_info);
				connection.close();
			}catch (TException e){
				System.out.println("Failed to set successor");
				e.printStackTrace();
			}
		}else{
			System.out.println("update the successor");
			System.out.println(successor_info);
			Node successor = GetNode(successor_info);
			this.finger_table[0] = successor;
		}
	}
	//set predecessor for current node
	void SetPredecessor(String predecessor_info){
		if (!ishost){
			try{
				TTransport connection =  new TSocket(this.host,this.port);
				connection.open();
				TProtocol protocol = new TBinaryProtocol(connection);
				NodeService.Client client_stub = new NodeService.Client(protocol);
				client_stub.SetPredecessor(predecessor_info);
				connection.close();
			}catch (TException e){
				System.out.println("Failed to set predecessor");
			}
		}else{
			this.predecessor = GetNode(predecessor_info);
		}
	}
	//return node object from information string
	public Node GetNode(String node){
		String[] information = node.split(",");
		int my_id = Integer.parseInt(information[0]);
		String my_host = information[1];
		int my_port = Integer.parseInt(information[2]);
		if (my_id != this.id){
			return new Node(my_id, my_host, my_port, false);
		}
		return this;
		
	}
	//update DHT of other node after a node finishes joining the system
	public void UpdateOthers(){
		for (int i = 0; i < m; i++){
			int distance = (int) (Math.pow(2,i));
			int id = this.id - distance + 1;
			if (id < 0){
				id = id + keyspace;
			}
			String predecessor_info = this.FindPredecessor(id);
			String[] tokens = predecessor_info.split(",");
			int predec_id = Integer.parseInt(tokens[0]);
			//if not current node and if the node has not already been updated in previous round
			if (predec_id != this.id){
				System.out.println("Broadcasting update to node " + predec_id);
				Node need_change = GetNode(predecessor_info);
				String current_info = this.id + "," + this.host + "," + this.port;
				need_change.UpdateDHT(current_info,i);
			}
			
		}
		System.out.println("Finish updating others");
	}
};
//Class where RPC will be invoked to handle request from client and supernode
public class NodeServiceHandler implements NodeService.Iface{
	SuperNodeService.Client supernode;
	Node node;
	String host;
	int port;
	//Constructor intialize node instance
	public NodeServiceHandler(String our_host, int our_port, String supernode_host, int supernode_port){
		this.host = our_host;
		this.port = our_port;
		
		try{
			//Ask to join the system, wait if there're other nodes joinging in
			System.out.println("super node host " + supernode_host);
			TTransport connection = new TSocket(supernode_host, supernode_port);
			connection.open();
			TProtocol protocol = new TBinaryProtocol(connection);
			this.supernode = new SuperNodeService.Client(protocol);
			System.out.println("Ask to join in the system");
			String message = supernode.getNodeForJoin(host, port);
			//can join the system immediately
			if (!message.equals("NACK")){
				this.JoinSystem(message);
				System.out.println("Done joining system");
				System.out.println("Finished announcing the supernode");
			}else{
				System.out.println("Waiting to join the system");
			}
		}catch (TException e){
			System.out.println("Failed to created connection with super node or joined the network");
			e.printStackTrace();
			
		}
	}
	//JoinSystem immediately, or after the super node is not occupied by other nodes
	public void JoinSystem(String message){
		System.out.println(message);
		String[] tokens = message.split(",");
		int id = Integer.parseInt(tokens[0]);
		this.node = new Node(id, host, port, true);
		node.JoinSystem(message);
		System.out.println("Done joining system");
		try{
			supernode.postJoin();
			System.out.println("Finished announcing the supernode");
		}catch(TException e){
			System.out.println("Failed to announce super node that it has finished");
		}
		
	}
	//put word to dictionary
	public boolean put(String word, String meaning, boolean sure){
		return node.put(word, meaning, sure);
		
	}
	//get word from dictionary
	public String get(String word, boolean sure){
		String meaning = node.get(word, sure);
		return meaning;
		
	}
	//get successor for the node
	public void SetSuccessor(String successor_info){
		node.SetSuccessor(successor_info);
		
	}
	//set predecessor for the ndoe
	public void SetPredecessor(String predecessor_info){
		node.SetPredecessor(predecessor_info);
		
	}
	//get successor for the node
	public String GetSuccessor(){
		return node.GetSuccessor();
	}
	//get predecessor for the node
	public String GetPredecessor(){
		return node.GetPredecessor();
	}
	//update other nodes when current node finish joining the system
	public void UpdateOthers(){
		node.UpdateOthers();
	}
	//update specific finger in DHT
	public void UpdateDHT(String new_successor, int index){
		node.UpdateDHT(new_successor, index);
	}
	//display current finger table
	public String DisplayFingerTable(){
		return node.DisplayFingerTable();
	}
	//find predecessor for a key
	public String FindPredecessor(int id){
		return node.FindPredecessor(id);
	}
	//find successor for a key
	public String FindSuccessor(int id){
		return node.FindSuccessor(id);
	}
	//return node  that closely precedes they provided id
	public String ClosestPrecedingFinger( int target_id){
		return node.ClosestPrecedingFinger(target_id);
	
	}
	
}
