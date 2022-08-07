namespace java supernode

service SuperNodeService {
    string getNodeForJoin(1: string ip, 2: i32 port),
    oneway void postJoin(), 
    string getNodeForClient(),
    string displayFingerTable()
}

