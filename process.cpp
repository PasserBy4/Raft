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
#include <random> 
#include <cmath>
#include <cstring>
#include <unistd.h>

enum class NodeState {
    Follower,
    Candidate,
    Leader
};

enum class MessageType {
    RequestVote,
    VoteResult,
    AppendEntries,
    AppendEntriesResponse,
    ClientMessage,
    GetChatLog,
    CrashProcess,
    CrashNotice,
    ForwardMsg,
    Unknown
};

struct Message {
    MessageType type;
    int messageID;
    int senderID;
    std::string content;
    int term;
};

struct LogEntry {
    int term;
    int messageID;
    std::string command;
    int confirmations = 1;
    LogEntry(int t, int messageID, std::string cmd): term(t), messageID(messageID), command(std::move(cmd)) {}
};

class StateMachine {
public:
    std::vector<std::string> chat_history;
    void apply(const std::string& command) {
        chat_history.push_back(command);
        std::cout << "applied to state machine: " << command << std::endl;
    }
};

class RaftNode {
private:
    NodeState state;
    StateMachine state_machine;
    int term;
    int id;
    int port;
    int base_port;
    int n_process;
    int alive_n_process;
    int leader_id = -1;
    int votes_received = 0;
    int vote_for = -1;
    int max_index = -1;
    bool reset_requested {false};
    bool stop {false};
    std::mutex mtx;
    std::mutex vr_mtx;
    std::condition_variable cv;
    std::condition_variable log_cv;
    std::vector<std::thread> threads;
    std::atomic<bool> runningHeartbeatTimer{false};
    std::thread heartbeat_thread;
    std::unordered_map<int, bool> node_status;
    std::vector<LogEntry> log;
    std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<std::mt19937::result_type> dist{1500, 3000};


    void heartBeatTimer() {
        std::cout << id << ": Heartbeat timer started." << std::endl;
        try {
            while(runningHeartbeatTimer) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                if (!runningHeartbeatTimer) {
                    std::cout << id << ": Heartbeat timer stopping as flag is false." << std::endl;
                    break;
                }
                sendHeartBeats();
            }
        } catch (const std::exception& e) {
            std::cerr << id << ": Exception in heartbeat timer: " << e.what() << std::endl;
        }
        std::cout << id << ": Heartbeat timer exited." << std::endl;
    }

    void startHeartbeatTimer() {
        runningHeartbeatTimer = true;
        heartbeat_thread = std::thread(&RaftNode::heartBeatTimer, this);
    }

    void stopHeartbeatTimer() {
        runningHeartbeatTimer = false;
        if(heartbeat_thread.joinable()) {
            heartbeat_thread.join();
        }
    }

    void sendMessageToNode(int node_id, const std::string& message, bool close_immediately = true, int timeout_ms = 1000, int max_retries = 3) {
        std::string state_str = (state == NodeState::Follower) ? "Follower" :
                                (state == NodeState::Candidate) ? "Candidate" : "Leader";
        if (message.find("heartbeat") == std::string::npos) {                        
            std::cout << "Node " << id << " (State: " << state_str << ", Term: " << term << ") sending message to node " << node_id << " : " << message<< std::endl;
        }
        
        std::string ip = "127.0.0.1";
        int port = base_port + node_id;
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::cerr << id << ": Socket creation failed." << std::endl;
            return;
        }

        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (const char*)&timeout, sizeof(timeout));

        struct sockaddr_in serv_addr;
        memset(&serv_addr, 0, sizeof(serv_addr));
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(port);
        if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
            std::cerr << id << ": Invalid address / Address not supported" << std::endl;
            close(sock);
            return;
        }

        int retry_count = 0;
        bool sent_successfully = false;

        while (retry_count < max_retries && !sent_successfully) {
            if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
                std::cerr << id << ": Connection to node " << node_id << " failed." << std::endl;
                retry_count++;
                std::cerr << "Retry " << retry_count << " of " << max_retries << std::endl;
                continue;
            }

            if (send(sock, message.c_str(), message.length(), 0) < 0) {
                std::cerr << id << ": Failed to send message to node " << node_id << "." << std::endl;
                retry_count++;
                std::cerr << "Retry " << retry_count << " of " << max_retries << std::endl;
            } else {
                sent_successfully = true;
            }
        }

        if (close_immediately) {
            close(sock);
        }

        if (!sent_successfully) {
            std::cerr << id << ": Failed to send message after " << max_retries << " attempts." << std::endl;
        }
    }
    
    void sendMessageToClient(int sock, std::string message){
        std::cout << id << " send message : " << message << " to proxy" << std::endl;
        message = message + "\n";
        if(send(sock, message.c_str(), message.length(), 0) < 0) {
            std::cerr << id << ": Failed to send message to client" << std::endl;
        }
    }

public:
    RaftNode(int id, int n_process, int port)
    :state(NodeState::Follower), term(0), id(id), port(port), n_process(n_process), base_port(port - id), vote_for(-1), stop(false){
        alive_n_process = n_process;
        for(int i = 0; i < n_process; i++){
            node_status[i] = true;
        }
    }

    ~RaftNode() {
        for(auto& th : threads) {
            if(th.joinable()) th.join();
        }
        if (heartbeat_thread.joinable()) {
            heartbeat_thread.join(); 
        }
    }

    void run() {
        threads.push_back(std::thread(&RaftNode::listenForConnections, this));
        threads.push_back(std::thread(&RaftNode::manageElections, this));
        if (state == NodeState::Leader) {
            threads.push_back(std::thread(&RaftNode::heartBeatTimer, this));
        }
    }

    void listenForConnections() {
        int server_fd, new_socket;
        struct sockaddr_in address;
        int addrlen = sizeof(address);

        if((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
            perror("socket failed");
            exit(EXIT_FAILURE);
        }

        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);

        if(bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0){
            perror("bind failed");
            exit(EXIT_FAILURE);
        }

        if(listen(server_fd, 10) < 0){
            perror("listen");
            exit(EXIT_FAILURE);
        }

        while(true) {
            if((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
                perror("accept");
                continue;
            }
            std::thread(&RaftNode::handleConnection, this, new_socket).detach();
        }
    }

    Message parseMessage(const char* buffer) {
        std::istringstream iss(buffer);
        std::string command;
        iss >> command;

        Message msg;

        if (command == "RequestVote") {
            msg.type = MessageType::RequestVote;
            iss >> msg.senderID >> msg.term >> msg.messageID;
        }
        else if (command == "VoteResult") {
            msg.type = MessageType::VoteResult;
            iss >> msg.senderID >> msg.term >> msg.content;
        }
        else if (command == "AppendEntries") {
            msg.type = MessageType::AppendEntries;
            iss >> msg.senderID >> msg.term;
            std::getline(iss, msg.content);
            if(!msg.content.empty() && msg.content[0] == ' ') {
                msg.content.erase(0, 1);
            }
        }
        else if (command == "AppendEntriesResponse") {
            msg.type = MessageType::AppendEntriesResponse;
            iss >> msg.senderID >> msg.term;
            std::getline(iss, msg.content);
            if(!msg.content.empty() && msg.content[0] == ' ') {
                msg.content.erase(0, 1);
            }
        }
        else if (command == "ForwardMsg") {
            msg.type = MessageType::ForwardMsg;
            iss >> msg.messageID;
            std::getline(iss, msg.content);
            if(!msg.content.empty() && msg.content[0] == ' ') {
                msg.content.erase(0, 1);
            }
        }
        else if (command == "msg") {
            msg.type = MessageType::ClientMessage;
            iss >> msg.messageID;
            std::getline(iss, msg.content);
            if(!msg.content.empty() && msg.content[0] == ' ') {
                msg.content.erase(0, 1);
            }
        }
        else if (command == "get") {
            msg.type = MessageType::GetChatLog;
        }
        else if (command == "crash") {
            msg.type = MessageType::CrashProcess;
        }
        else if (command == "CrashNotice") {
            msg.type = MessageType::CrashNotice;
            iss >> msg.senderID;
        }
        else {
            msg.type = MessageType::Unknown;
        }
        return msg;
    }

    void updateTerm(int receivedTerm) {
        std::unique_lock<std::mutex> lock(vr_mtx);
        if (receivedTerm > term) {
            loseLeadership();
            term = receivedTerm;
            vote_for = -1;
            votes_received = 0;
            state = NodeState::Follower;
            loseLeadership();
        }
    }
    
    void handleRequestVote(const Message& msg, int sock) {
        try{
            std::unique_lock<std::mutex> lock(mtx);
            reset_requested = true;
            cv.notify_all();
            if (msg.term > term) {
                updateTerm(msg.term);
            }

            std::string response = "VoteResult " + std::to_string(id) + " " + std::to_string(term) + " ";

            if (msg.term >= term && (vote_for == -1 || vote_for == msg.senderID)) {
                vote_for = msg.senderID;
                response += "granted";
                std::cout << id << ": grant " << msg.senderID << std::endl;
            }
            else {
                response += "denied";
                std::cout << id << ": deny " << msg.senderID << std::endl;
            }
            // lock.unlock();
            // send(sock, response.c_str(), response.length(), 0);
            // close(sock);
            sendMessageToNode(msg.senderID, response);
        } catch (const std::exception& e) {
            std::cerr << "Exception in handleRequestVote: " << e.what() << std::endl;
        }
    }

    void handleVoteResult(const Message& msg) {
        try {
            std::unique_lock<std::mutex> lock(mtx);
            if(msg.term != term || state != NodeState::Candidate) {
                return;
            }
            if(msg.content == "granted") {
                {
                    std::unique_lock<std::mutex> lock(vr_mtx);
                    votes_received++;
                }
                std::cout << id << ": received vote: " << votes_received << " out of " << (alive_n_process / 2 + 1) << " needed with term " << msg.term << std:: endl;

                if(votes_received > alive_n_process / 2) {
                    becomeLeader();
                    // reset_requested = true;
                    // cv.notify_one();
                }
            }
            else {
                std::cout << id << ": vote denied by " << msg.senderID << " with term " << msg.term << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Exception in handleVoteResult: " << e.what() << std::endl;
        }
    }

    void handleAppendEntries(const Message& msg) {
        try {
            std::unique_lock<std::mutex> lock(mtx);
            if(msg.term >= term) {
                if(msg.term > term) {
                    updateTerm(msg.term); 
                }
                state = NodeState::Follower;
                leader_id = msg.senderID;
                reset_requested = true;
                cv.notify_all();
                
                if(msg.content == "heartbeat") {
                    // std::string state_str = (state == NodeState::Follower) ? "Follower" :
                    //         (state == NodeState::Candidate) ? "Candidate" : "Leader";
                    // std::cout << "Node " << id << " (State: " << state_str << ", Term: " << term << ")"  << std::endl;
                    // std::cout << id << ": received heartbeat from node " << msg.senderID << std::endl;
                    // sendAppendEntriesResponse(msg.senderID, true);
                }
                else {
                    std::istringstream iss(msg.content);
                    // std::string command;
                    // int entryTerm;
                    // while(iss >> entryTerm >> command) {
                    //     if(log.empty() || log.back().term <= entryTerm) {
                    //         log.push_back(LogEntry(entryTerm, command));
                    //         sendAppendEntriesResponse(msg.senderID, true);
                    //     } else {
                    //         sendAppendEntriesResponse(msg.senderID, false);
                    //     }
                    // }
                    std::string type;
                    int message_term, index, message_id;
                    iss >> type;
                    if(type == "entry") {
                        iss >> message_term >> index >> message_id;
                        std::string content;
                        std::getline(iss, content);
                        if(!content.empty() && content[0] == ' '){
                            content = content.substr(1);
                        }
                        if(log.empty() || log.back().term <= message_term) {
                            log.push_back(LogEntry(message_term, message_id, content));
                            sendAppendEntriesResponse(msg.senderID, true, index);
                        }
                        else {
                            sendAppendEntriesResponse(msg.senderID, false);
                        }
                    }
                    else if(type == "commit") {
                        iss >> index;
                        applyLogToStateMachine(index);
                    }

                }
            }
            else {
                sendAppendEntriesResponse(msg.senderID, false);
            }
        } catch (const std::exception& e) {
            std::cerr << "Exception in handleAppendEntries: " << e.what() << std::endl;
        }
    }

    void sendAppendEntriesResponse(int leader_id, bool success, int index=-1) {
        std::string message = "AppendEntriesResponse " + std::to_string(id) + " " + std::to_string(term) + " " + (success ? "success" : "fail");
        if (index != -1) {
            message += " " + std::to_string(index); 
        }
        sendMessageToNode(leader_id, message);
    }

    void handleAppendEntriesResponse(const Message& msg) {
        if (msg.content.substr(0, 7) == "success") {
            size_t space_pos = msg.content.find_last_of(' ');
            if (space_pos != std::string::npos) {
                std::string index_str = msg.content.substr(space_pos + 1);
                if (!index_str.empty()) {
                    int index = std::stoi(index_str);
                    if (index < log.size()) {
                        log[index].confirmations++;
                        if (log[index].confirmations > alive_n_process / 2) {
                            log_cv.notify_all();
                        }
                    }
                }
            }
        }
    }

    void applyLogToStateMachine(int index) {
        if (index < log.size() && log[index].term == term) {
            state_machine.apply(log[index].command);
            max_index = std::max(index, max_index);
            log_cv.notify_all();
            std::cout << id << ": log entry applied at index " << index << ": " << log[index].command << std::endl;
        }
    }

    void handleClientMessage(const Message& msg, int sock=-1) {
        try {
            std::cout << id << ": try to handle message: " << msg.content << "from client" << std::endl;
            std::unique_lock<std::mutex> lock(mtx);
            if(state == NodeState::Leader) {
                log.push_back(LogEntry(term, msg.messageID, msg.content));
                int current_index = log.size() - 1;

                log[current_index].confirmations = 1;

                for(int i = 0; i < n_process; i++) {
                    if(node_status[i] == false) continue;
                    if(i != id) {
                        std::string content = "entry " + std::to_string(term) + " " + std::to_string(current_index) + " " + std::to_string(msg.messageID) + " " + msg.content;
                        sendAppendEntries(i, false, content);
                    }
                }

                log_cv.wait(lock, [this, current_index](){return log[current_index].confirmations > alive_n_process / 2; });
                applyLogToStateMachine(current_index);
                for(int i = 0; i < n_process; i++) {
                    if(node_status[i] == false) continue;
                    if(i != id) {
                        std::string content = "commit " + std::to_string(current_index);
                        sendAppendEntries(i, false, content);
                    }
                }
                if (sock != -1) {
                    std::string response = "ack " + std::to_string(msg.messageID) + " " + std::to_string(current_index);
                    sendMessageToClient(sock, response);
                }

            }
            else {
                std::string forward_message = "ForwardMsg " + std::to_string(msg.messageID) + msg.content; 
                int current_index = log.size();
                sendMessageToNode(leader_id, forward_message);
                log_cv.wait(lock, [this, current_index](){ return max_index >= current_index; });
                if(sock != -1) {
                    std::string response = "ack " + std::to_string(msg.messageID) + " " + std::to_string(current_index);
                    sendMessageToClient(sock, response);
                }
                
            }
        } catch (const std::exception& e) {
            std::cerr << "Exception in handleClientMessage: " << e.what() << std::endl;
        }
    }

    void handleGetChatLog(int sock) {
        std::ostringstream oss;
        for(const auto& message : state_machine.chat_history) {
            if (oss.tellp() > 0) {
                oss << ",";
            }
            oss << message;
        }
        std::string chatLog = oss.str();
        std::string message = "chatLog " + chatLog;
        sendMessageToClient(sock, message);
    }

    void handleCrashProcess() {
        for (int i = 0; i < n_process; i++) {
            if (node_status[i] == false) continue;
            std::string message = "CrashNotice " + std::to_string(id);
            sendMessageToNode(i, message);
        }
        stop = true;
        stopHeartbeatTimer();
        for(auto& th : threads) {
            if (th.joinable()) {
                th.join();
            }
        }
        if (heartbeat_thread.joinable()) {
            heartbeat_thread.join(); 
        }
    }

    void handleCrashNotice(const Message& msg) {
        node_status[msg.senderID] = false;
        alive_n_process--;
    }
    void handleConnection(int sock) {
        char buffer[1024] {0};
        while(true) {
            read(sock, buffer, 1024);
            Message msg = parseMessage(buffer);
            bool from_proxy = false;
            switch(msg.type) {
                case MessageType::RequestVote:
                    handleRequestVote(msg, sock);
                    break;
                case MessageType::VoteResult:
                    handleVoteResult(msg);
                    break;
                case MessageType::AppendEntries:
                    handleAppendEntries(msg);
                    break;
                case MessageType::AppendEntriesResponse:
                    handleAppendEntriesResponse(msg);
                    break;
                case MessageType::ClientMessage:
                    handleClientMessage(msg, sock);
                    from_proxy = true;
                    break;
                case MessageType::ForwardMsg:
                    handleClientMessage(msg);
                    break;
                case MessageType::GetChatLog:
                    handleGetChatLog(sock);
                    from_proxy = true;
                    break;
                case MessageType::CrashProcess:
                    handleCrashProcess();
                    from_proxy = true;
                    break;
                case MessageType::CrashNotice:
                    handleCrashNotice(msg);
                    break;
                default:
                    std::cerr << id << ": received unknown message type." << std::endl;
            }
            if(!from_proxy){
                break;
            }
        }
        close(sock);
    }

    
    void manageElections() {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        while(true) {
            std::unique_lock<std::mutex> lock(mtx);
            int timeout = dist(rng);
            if (cv.wait_for(lock, std::chrono::milliseconds(timeout), [this]() { return reset_requested || stop; })) {
                // std::cout << id << ": Election timer triggered reset or stop. Reset: " << reset_requested << " Stop: " << stop << std::endl;
                if (stop) break;
                reset_requested = false;
            } else {
                // std::cout << id << ": Election timeout, becoming a candidate." << std::endl;
                if (state == NodeState::Follower) {
                    mtx.unlock();
                    becomeCandidate();
                }
            }
        }
    }

    void sendHeartBeats() {
        if (state != NodeState::Leader) {
            return;
        }
        for (int i = 0; i < n_process; i++) {
            if (node_status[i] == false) continue;
            if (i != id) {
                sendAppendEntries(i, true);
            }
        }
    }

    void sendAppendEntries(int node_id, bool heartbeat, const std::string& content = "") {
        try {
            std::string message;
            if (heartbeat) {
                message = "AppendEntries " + std::to_string(id) + " " + std::to_string(term) + " heartbeat";
            } else {
                std::ostringstream oss;
                oss << "AppendEntries " << id << " " << term << " ";
                oss << content;
                message = oss.str();
            }
            sendMessageToNode(node_id, message);
        } catch (const std::exception& e) {
            std::cerr << "Exception in sendAppendEntries: " << e.what() << std::endl;
        }
    }

    void becomeCandidate() {
        state = NodeState::Candidate;
        std::cout << id << ": becoming candidate" << std::endl;
        stopHeartbeatTimer();
        requestVotes();
    }

    void becomeLeader() {
        state = NodeState::Leader;
        std::cout << id << ": becoming leader for term " << term << std::endl;
        startHeartbeatTimer();
    }

    void loseLeadership() {
        state = NodeState::Follower;
        std::cout << id << ": loss leadership for term " << term << std::endl;
        stopHeartbeatTimer();
    }

    void sendRequestVote (int node_id) {
        std::string message = "RequestVote " + std::to_string(id) + " " + std::to_string(term) + " " + std::to_string(rand());
        sendMessageToNode(node_id, message);
    }

    void requestVotes() {
        std::lock_guard<std::mutex> lock(mtx);
        term++;
        {
            std::unique_lock<std::mutex> lock(vr_mtx);
            votes_received = 1;
            vote_for = id;
        }
        std::cout << id << ": requesting votes for term " << term << std::endl;
        for(int i = 0; i < n_process; i++){
            if (node_status[i] == false) continue;
            if(i != id) {
                sendRequestVote(i);
            }
        }
    }
};

int main(int argc, char* argv[]){
    if(argc != 4){
        std::cerr << "Usage: <processID> start <nProcesses> <portNo>" << std::endl;
        return 1;
    }
    int process_id = std::stoi(argv[1]);
    int n_process = std::stoi(argv[2]);
    int port = std::stoi(argv[3]);
    RaftNode node(process_id, n_process, port);
    node.run();
    return 0;

}