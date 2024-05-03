#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <sstream>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

enum class NodeState {
    Follower,
    Candidate,
    Leader
;}

class RaftNode {
private:
    NodeState state;
    int term;
    std::mutex mtx;
    std::condition_variable cv;
}
int main(int argc, char* argv[]){}{
    if(argc != 4){
        cerr << "Usage: <processID> start <nProcesses> <portNo>" << endl;
        return 1;
    }
    int nProcesses = stoi(argv[2]);
    int portNo = stoi(argv[3]);

}