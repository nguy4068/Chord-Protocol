namespace java node 
service NodeService{
	bool put(1: string word, 2: string meaning, 3: bool sure),
	string get(1:string word, 2: bool sure),
	void UpdateDHT(1: string new_successor, 2: i32 index),
	oneway void JoinSystem(1: string info),
	string FindSuccessor(1: i32 id),
	string FindPredecessor(1: i32 id),
	string ClosestPrecedingFinger(1: i32 id)
	string GetPredecessor(),
	string GetSuccessor(),
	void SetPredecessor(1: string predecessor_info),
	void SetSuccessor(1: string successor_info),
	string DisplayFingerTable()
}
