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

using namespace std;

enum class NodeState {
    Follower,
    Candidate,
    Leader
};

struct Message {
    std::string text;
    int term;
    std::string sender;
};

const int BASE_PORT = 20000;
vector<Message> log;
int term = 0;
NodeState currentState = NodeState::Follower;
int currentLeader = -1;
mutex mtx;
condition_variable cv;
int nodeId;
unordered_map<int, string> peers;
int MAX_PEERS;

void becomeCandidate();
void becomeLeader();
void election();
void handleClient(int sock);
void handleConnection(int sock);
void receiveMessage(int sock, Message& msg);
void sendMessage(int sock, const Message& msg);
void startServer(int port);
bool isPortActive(int port);
void sendHeartbeats();

class RaftNode {
private:
    NodeState state;
    int term;
    mutex mtx;
    condition_variable cv;
};

int main(int argc, char* argv[]) {
    if(argc != 4){
        cerr << "Usage: <processID> start <nProcesses> <portNo>" << endl;
        return 1;
    }
    int nProcesses = stoi(argv[2]);
    int portNo = stoi(argv[3]);
    nodeId = portNo;
    MAX_PEERS = nProcesses;

    startServer(portNo);

    return 0;
}

void startServer(int port) {
    int serverSock = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSock < 0) {
        cerr << "Error creating socket." << endl;
        exit(1);
    }

    sockaddr_in serverAddr{};
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (::bind(serverSock, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        cerr << "Error binding socket." << endl;
        close(serverSock);
        exit(1);
    }

    if (listen(serverSock, 10) < 0) {
        cerr << "Error listening on socket." << endl;
        close(serverSock);
        exit(1);
    }

    cout << "Server started on port " << port << ". State: FOLLOWER" << endl;


    while(true) {
        sockaddr_in clientAddr{};
        socklen_t clientAddrSize = sizeof(clientAddr);
        int clientSock = accept(serverSock, (struct sockaddr*)&clientAddr, &clientAddrSize);
        if (clientSock < 0) {
            cerr << "Error accepting connection." << endl;
            exit(1);
        }
        thread clientThread(handleConnection, clientSock);
        clientThread.detach();

        if (currentState == NodeState::Follower) {
            unique_lock<mutex> lock(mtx);
            if (cv.wait_for(lock, chrono::seconds(3), [] { return currentState != NodeState::Follower; })) {
                continue;
            } else {
                becomeCandidate();
            }
        } else if (currentState == NodeState::Leader) {
            replicateLog();
        }
    }
}

void election() {
    std::unique_lock<std::mutex> lock(mtx);
    int votesReceived = 1;
    int activePortCount = 0;
    for (int port = BASE_PORT; port < BASE_PORT + MAX_PEERS; port++) {
        if (isPortActive(port)) {
            activePortCount++;
        }
    }

    int majority = (activePortCount / 2) + 1;

    for (auto& peer : peers) {
        if (peer.first == nodeId) continue;

        int sock = createSocket(peer.second, peer.first);
        if (sock == -1) continue;

        Message voteRequest;
        voteRequest.term = term;
        voteRequest.sender = to_string(nodeId);
        voteRequest.text = "RequestVote";
        sendMessage(sock, voteRequest);

        Message voteResponse;
        receiveMessage(sock, voteResponse);
        if (voteResponse.text == "VoteGranted" && voteResponse.term == term) {
            votesReceived++;
        }

        close(sock);
        if (votesReceived >= majority) {
            becomeLeader();
            cv.notify_all();
            break;
        }
    }
    if (currentState != NodeState::Leader) {
        currentState = NodeState::Follower;
    }
    lock.unlock();
}

int createSocket(const string& ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    return sock;
}

void replicateLog() {
    while (currentState == NodeState::Leader) {
        for (auto& peer : peers) {
            if (peer.first == nodeId) continue;

            int sock = createSocket(peer.second, peer.first);
            if (sock == -1) continue;

            for (const auto& message : log) {
                sendMessage(sock, message);
            }

            close(sock);
        }
        this_thread::sleep_for(chrono::milliseconds(100));
    }
}

bool isPortActive(int port) {
    int testSock = socket(AF_INET, SOCK_STREAM, 0);
    if (testSock < 0) {
        cerr << "Error creating socket for testing port activity." << endl;
        return false;
    }

    sockaddr_in testAddr{};
    testAddr.sin_family = AF_INET;
    testAddr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &testAddr.sin_addr);

    // Attempt to connect to the port
    bool active = connect(testSock, (sockaddr*)&testAddr, sizeof(testAddr)) >= 0;
    close(testSock); // Close the socket after testing
    return active;
}

void becomeLeader() {
    lock_guard<mutex> guard(mtx);
    currentState = NodeState::Leader;
    cout << "Becoming Leader. Term: " << term << endl;
    // log replication
}

void becomeCandidate() {
    lock_guard<mutex> guard(mtx);
    currentState = NodeState::Candidate;
    term++;
    cout << "Becoming Candidate. Term: " << term << endl;
    election();
}

void sendHeartbeats() {
    while (currentState == NodeState::Leader) {
        for (auto& peer : peers) {
            if (peer.first == nodeId) continue;

            int sock = createSocket(peer.second, peer.first);
            if (sock == -1) continue;

            Message heartbeat;
            heartbeat.term = term;
            heartbeat.sender = to_string(nodeId);
            heartbeat.text = "Heartbeat";
            sendMessage(sock, heartbeat);

            close(sock);
        }
        this_thread::sleep_for(chrono::milliseconds(50));
    }
}

void handleVoteRequest(int sock, const Message& msg) {
    Message response;
    response.sender = to_string(nodeId);
    unique_lock<mutex> lock(mtx);

    if (msg.term > term) {
        term = msg.term;
        currentState = NodeState::Follower;
        response.text = "VoteGranted";
    } else {
        response.text = "VoteDenied";
    }
    
    sendMessage(sock, response);
}