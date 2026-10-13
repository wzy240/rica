/*
 * SPDX-License-Identifier: GPL-2.0-only
 * PA-ATP Large-scale Simulation Setup
 * - 8x8 Leaf-Spine, 128 Hosts
 * - 8 Jobs (4x ResNet50, 4x VGG19), 12 workers each
 * - Iterative Training (Compute -> Communicate barrier)
 * - Deterministic Routing (ARO) via JSON
 */

#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include "ns3/packet-sink.h"
#include "ns3/point-to-point-module.h"
#include "ns3/atp-module.h"
#include "ns3/ptr.h"
#include <fstream>
#include <string>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <map>
#include <vector>
#include <set>

using namespace ns3;
using namespace std;

NS_LOG_COMPONENT_DEFINE("ATP-SpineLeaf-JSON-Iterative");

// 网络拓扑结构 (符合原论文 8 Spine, 8 Leaf, 每 Leaf 16 Server)
struct NetworkTopology {
    static const uint32_t nSpines = 8;
    static const uint32_t nLeaves = 8; 
    static const uint32_t nServersPerLeaf = 16;
    static const uint32_t nTotalServers = nLeaves * nServersPerLeaf;
    static const uint32_t nTotalNodes = nSpines + nLeaves + nTotalServers;
    
    // 节点索引分配
    static const uint32_t spineStartIdx = 0;
    static const uint32_t leafStartIdx = spineStartIdx + nSpines;
    static const uint32_t serverStartIdx = leafStartIdx + nLeaves;
};

// JSON 解析的 Job 配置结构
struct JobConfig {
    uint32_t id;
    std::string model;
    std::vector<std::string> workers;
    std::string ps;
    std::string ps_leaf;
};

// 路由配置结构 (ARO)
struct RouteConfig {
    std::map<uint32_t, std::vector<std::pair<std::string, std::string>>> jobRoutes;
};

// 新增：运行时状态与迭代控制结构
struct DmlJobRuntime {
    uint32_t id;
    std::string modelName;
    uint64_t modelSizeBytes;
    Time computeTime;
    
    uint32_t currentIteration;
    uint32_t maxIterations;
    
    Ptr<ATPPacketSink> psSinkApp;
    std::vector<Ptr<ATPBulkSendApplication>> workerApps;
};

std::map<std::string, uint32_t> nodeNameToIndex;
std::map<uint32_t, std::string> nodeIndexToName;
std::vector<DmlJobRuntime> g_runtimeJobs; // 存放所有任务的运行时状态

// 全局变量用于跟踪
std::ofstream cwndStream_job1, cwndStream_job2;
std::ofstream SinkBytesStream_job1, SinkBytesStream_job2;
uint64_t lastTimeJob1Bytes = 0, lastTimeJob2Bytes = 0;
uint8_t numJobs = 8;

// ======================= JSON 解析辅助函数 =======================
std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, (last - first + 1));
}

std::string stripQuotes(const std::string& str) {
    if (str.size() >= 2 && str.front() == '"' && str.back() == '"') {
        return str.substr(1, str.size() - 2);
    }
    return str;
}

std::vector<std::string> parseStringArray(const std::string& arrayStr) {
    std::vector<std::string> result;
    std::string content = arrayStr;
    size_t start = content.find('[');
    size_t end = content.find_last_of(']');
    if (start != std::string::npos && end != std::string::npos) {
        content = content.substr(start + 1, end - start - 1);
    }
    std::stringstream ss(content);
    std::string item;
    while (std::getline(ss, item, ',')) {
        item = trim(item);
        item = stripQuotes(item);
        if (!item.empty()) result.push_back(item);
    }
    return result;
}

std::vector<JobConfig> parseJobsConfig(const std::string& filename) {
    std::vector<JobConfig> jobs;
    std::ifstream file(filename);
    if (!file.is_open()) NS_FATAL_ERROR("Cannot open jobs config file: " << filename);
    
    std::string line;
    JobConfig currentJob;
    bool inJob = false;
    bool inWorkers = false;
    std::string workersContent;
    
    while (std::getline(file, line)) {
        line = trim(line);
        if (line.find("\"id\":") != std::string::npos) {
            size_t pos = line.find(':');
            if (pos != std::string::npos) {
                std::string idStr = trim(line.substr(pos + 1));
                if (idStr.back() == ',') idStr.pop_back();
                currentJob.id = std::stoi(stripQuotes(idStr));
                inJob = true;
            }
        } else if (line.find("\"model\":") != std::string::npos) {
            size_t pos = line.find(':');
            if (pos != std::string::npos) {
                std::string modelStr = trim(line.substr(pos + 1));
                if (modelStr.back() == ',') modelStr.pop_back();
                currentJob.model = stripQuotes(modelStr);
            }
        } else if (line.find("\"workers\":") != std::string::npos) {
            inWorkers = true;
            size_t pos = line.find('[');
            if (pos != std::string::npos) workersContent = line.substr(pos);
        } else if (inWorkers && line.find(']') != std::string::npos) {
            if (workersContent.find(']') == std::string::npos) workersContent += line;
            currentJob.workers = parseStringArray(workersContent);
            inWorkers = false;
            workersContent.clear();
        } else if (inWorkers) {
            workersContent += line;
        } else if (line.find("\"ps\":") != std::string::npos) {
            size_t pos = line.find(':');
            if (pos != std::string::npos) {
                std::string psStr = trim(line.substr(pos + 1));
                if (psStr.back() == ',') psStr.pop_back();
                currentJob.ps = stripQuotes(psStr);
            }
        } else if (line.find("\"ps_leaf\":") != std::string::npos) {
            size_t pos = line.find(':');
            if (pos != std::string::npos) {
                std::string psLeafStr = trim(line.substr(pos + 1));
                if (psLeafStr.back() == ',') psLeafStr.pop_back();
                currentJob.ps_leaf = stripQuotes(psLeafStr);
            }
        } else if ((line == "}" || line == "},") && inJob) {
            jobs.push_back(currentJob);
            currentJob = JobConfig();
            inJob = false;
        }
    }
    return jobs;
}

RouteConfig parseRouteConfig(const std::string& filename) {
    RouteConfig config;
    std::ifstream file(filename);
    if (!file.is_open()) NS_FATAL_ERROR("Cannot open route config file: " << filename);
    
    std::string line;
    uint32_t currentJobId = 0;
    bool inJobRoutes = false;
    
    while (std::getline(file, line)) {
        line = trim(line);
        // 查找 job ID
        for (int i = 1; i <= numJobs; ++i) {
            std::string key = "\"" + std::to_string(i) + "\":";
            if (line.find(key) != std::string::npos) {
                currentJobId = i;
                inJobRoutes = true;
                break;
            }
        }
        
        if (inJobRoutes && line.find('[') != std::string::npos && line.find(']') != std::string::npos && line.find("leaf") != std::string::npos) {
            size_t start = line.find('[');
            size_t end = line.find(']');
            if (start != std::string::npos && end != std::string::npos) {
                std::string routeStr = line.substr(start + 1, end - start - 1);
                std::vector<std::string> routePair = parseStringArray("[" + routeStr + "]");
                if (routePair.size() == 2) {
                    config.jobRoutes[currentJobId].push_back({routePair[0], routePair[1]});
                }
            }
        } else if (line == "]," || line == "]") {
            inJobRoutes = false;
        }
    }
    return config;
}

// ======================= 拓扑与地址分配辅助函数 =======================
void initializeNodeMapping() {
    for (uint32_t i = 0; i < NetworkTopology::nSpines; i++) {
        std::string name = "spine" + std::to_string(i + 1);
        uint32_t index = NetworkTopology::spineStartIdx + i;
        nodeNameToIndex[name] = index;
        nodeIndexToName[index] = name;
    }
    for (uint32_t i = 0; i < NetworkTopology::nLeaves; i++) {
        std::string name = "leaf" + std::to_string(i + 1);
        uint32_t index = NetworkTopology::leafStartIdx + i;
        nodeNameToIndex[name] = index;
        nodeIndexToName[index] = name;
    }
    for (uint32_t i = 0; i < NetworkTopology::nTotalServers; i++) {
        std::string name = "server" + std::to_string(i + 1);
        uint32_t index = NetworkTopology::serverStartIdx + i;
        nodeNameToIndex[name] = index;
        nodeIndexToName[index] = name;
    }
}

uint32_t getServerLeafIndex(uint32_t serverIndex) {
    return (serverIndex - NetworkTopology::serverStartIdx) / NetworkTopology::nServersPerLeaf;
}

uint32_t getServerLocalIndex(uint32_t serverIndex) {
    return (serverIndex - NetworkTopology::serverStartIdx) % NetworkTopology::nServersPerLeaf;
}

Ipv4Address GenerateMulticastAddress(uint32_t jobId) {
    return Ipv4Address((225 << 24) | (1 << 16) | (jobId << 8) | 1);
}

// ======================= 核心：迭代控制器 =======================
class DmlJobCoordinator {
public:
    static void CheckIterationProgress(uint32_t jobId) {
        DmlJobRuntime& job = g_runtimeJobs[jobId - 1];
        
        if (job.currentIteration >= job.maxIterations) return;

        // 检查 PS (Sink) 是否已经收到了当前迭代的完整模型大小
        uint64_t currentRx = job.psSinkApp->GetTotalRxJob(job.id);
        uint64_t targetRx = job.modelSizeBytes * (job.currentIteration + 1);

        if (currentRx >= targetRx) {
            // 停止当前 Job 的所有 Worker 发送数据
            for (auto& workerApp : job.workerApps)
            {
                workerApp->SetMaxBytes(currentRx);
            }
            job.currentIteration++;
            std::cout << "Time: " << std::fixed << std::setprecision(3) << Simulator::Now().GetSeconds() 
                      << "s | Job " << job.id << " (" << job.modelName 
                      << ") completed Iteration " << job.currentIteration 
                      << "/" << job.maxIterations << std::endl;
            
            if (job.currentIteration < job.maxIterations) {
                // 调度 GPU 计算时间，计算结束后触发下一轮通信
                Simulator::Schedule(job.computeTime, &DmlJobCoordinator::StartCommunicationPhase, job.id);
            } else {
                std::cout << ">>> Job " << job.id << " finished all training iterations." << std::endl;
            }
        } else {
            // 未完成聚合，1毫秒后再次轮询
            Simulator::Schedule(MilliSeconds(1), &DmlJobCoordinator::CheckIterationProgress, jobId);
        }
    }

    static void StartCommunicationPhase(uint32_t jobId) {
        DmlJobRuntime& job = g_runtimeJobs[jobId - 1];
        
        // 唤醒当前 Job 的所有 Worker 开始发送新一轮的数据
        for (auto& workerApp : job.workerApps) {
            // 动态增加允许发送的最大字节数
            workerApp->SetMaxBytes(0); 
        }
        
        // 开始监控这一轮的完成情况
        Simulator::Schedule(MilliSeconds(1), &DmlJobCoordinator::CheckIterationProgress, jobId);
    }
};

// 测量函数
static void
Measurement(Ptr<ATPPacketSink> sink1, Ptr<ATPPacketSink> sink2)
{
    Time now = Simulator::Now();

    uint64_t currentTimeJob1Bytes = sink1->GetTotalRxJob(1);
    uint64_t currentTimeJob2Bytes = sink2->GetTotalRxJob(3);

    uint64_t ReceivedJob1BytesPer100ms = currentTimeJob1Bytes - lastTimeJob1Bytes;
    uint64_t ReceivedJob2BytesPer100ms = currentTimeJob2Bytes - lastTimeJob2Bytes;

    SinkBytesStream_job1 << now.GetMicroSeconds() << "\t" << currentTimeJob1Bytes << "\t"
                         << ReceivedJob1BytesPer100ms << std::endl;
    SinkBytesStream_job2 << now.GetMicroSeconds() << "\t" << currentTimeJob2Bytes << "\t"
                         << ReceivedJob2BytesPer100ms << std::endl;

    lastTimeJob1Bytes = currentTimeJob1Bytes;
    lastTimeJob2Bytes = currentTimeJob2Bytes;

    Simulator::Schedule(MicroSeconds(100), &Measurement, sink1, sink2);
}

static void CwndChange_job1(uint32_t oldCwnd, uint32_t newCwnd) {
    cwndStream_job1 << Simulator::Now().GetMicroSeconds() << "\t" << newCwnd << std::endl;
}

static void CwndChange_job2(uint32_t oldCwnd, uint32_t newCwnd) {
    cwndStream_job2 << Simulator::Now().GetMicroSeconds() << "\t" << newCwnd << std::endl;
}

int main(int argc, char* argv[]) {
    std::string jobsConfigFile = "contrib/atp/examples/pa_atp_jobs.json";
    std::string routeConfigFile = "contrib/atp/examples/pa_atp_routes.json";
    
    CommandLine cmd;
    cmd.AddValue("jobsConfig", "Jobs configuration file", jobsConfigFile);
    cmd.AddValue("routeConfig", "Route configuration file", routeConfigFile);
    cmd.Parse(argc, argv);
    
    // 打开跟踪文件
    cwndStream_job1.open("atp-result/trace-spineleaf/job1-cwnd-trace.txt", std::ofstream::out | std::ofstream::trunc);
    cwndStream_job2.open("atp-result/trace-spineleaf/job3-cwnd-trace.txt", std::ofstream::out | std::ofstream::trunc);
    SinkBytesStream_job1.open("atp-result/trace-spineleaf/job1-sinkBytes-trace.txt", std::ofstream::out | std::ofstream::trunc);
    SinkBytesStream_job2.open("atp-result/trace-spineleaf/job3-sinkBytes-trace.txt", std::ofstream::out | std::ofstream::trunc);
    
    initializeNodeMapping();
    
    // 1. 解析配置文件并初始化运行时状态
    std::vector<JobConfig> jobs = parseJobsConfig(jobsConfigFile);
    RouteConfig routeConfig = parseRouteConfig(routeConfigFile);

    if (jobs.size() > numJobs)
    {
        jobs.resize(numJobs);
    }
    
    for (const auto& parsedJob : jobs) {
        DmlJobRuntime runtimeJob;
        runtimeJob.id = parsedJob.id;
        runtimeJob.currentIteration = 0;
        runtimeJob.maxIterations = 5; // 设置仿真 5 轮迭代
        
        // 分配论文中设定的精确模型大小和 GPU 延迟
        if (parsedJob.model == "ResNet50") {
            runtimeJob.modelName = "ResNet50";
            runtimeJob.modelSizeBytes = 98 * 1024 * 1024; // 98 MB
            runtimeJob.computeTime = MilliSeconds(50);
        } else {
            runtimeJob.modelName = "VGG19";
            runtimeJob.modelSizeBytes = 548 * 1024 * 1024; // 548 MB
            runtimeJob.computeTime = MilliSeconds(150);
        }
        g_runtimeJobs.push_back(runtimeJob);
    }
    
    // 仿真参数
    Time stopTime = Seconds(2.0); // 延长仿真时间以允许迭代完成
    uint32_t initCwnd = 32;
    cwndStream_job1 << 0 << "\t" << initCwnd << std::endl;
    cwndStream_job2 << 0 << "\t" << initCwnd << std::endl;
    
    NS_LOG_INFO("Create nodes.");
    NodeContainer nodes;
    nodes.Create(NetworkTopology::nTotalNodes);
    
    NodeContainer spineNodes, leafNodes, serverNodes;
    for (uint32_t i = 0; i < NetworkTopology::nSpines; i++) spineNodes.Add(nodes.Get(NetworkTopology::spineStartIdx + i));
    for (uint32_t i = 0; i < NetworkTopology::nLeaves; i++) leafNodes.Add(nodes.Get(NetworkTopology::leafStartIdx + i));
    for (uint32_t i = 0; i < NetworkTopology::nTotalServers; i++) serverNodes.Add(nodes.Get(NetworkTopology::serverStartIdx + i));
    
    NS_LOG_INFO("Create channels.");
    
    // 点对点链路配置 (100Gbps, 2us delay)
    PointToPointHelper p2pSpineLeaf;
    p2pSpineLeaf.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    p2pSpineLeaf.SetChannelAttribute("Delay", StringValue("2us"));
    p2pSpineLeaf.SetQueue("ns3::DropTailQueue", "MaxSize", StringValue("256000B"));
    
    PointToPointHelper p2pLeafServer;
    p2pLeafServer.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    p2pLeafServer.SetChannelAttribute("Delay", StringValue("2us"));
    p2pLeafServer.SetQueue("ns3::DropTailQueue", "MaxSize", StringValue("256000B"));
    
    std::vector<std::vector<NetDeviceContainer>> spineLeafDevices(NetworkTopology::nSpines, std::vector<NetDeviceContainer>(NetworkTopology::nLeaves));
    std::vector<std::vector<Ipv4InterfaceContainer>> spineLeafInterfaces(NetworkTopology::nSpines, std::vector<Ipv4InterfaceContainer>(NetworkTopology::nLeaves));
    std::vector<std::vector<NetDeviceContainer>> leafServerDevices(NetworkTopology::nLeaves, std::vector<NetDeviceContainer>(NetworkTopology::nServersPerLeaf));
    std::vector<std::vector<Ipv4InterfaceContainer>> leafServerInterfaces(NetworkTopology::nLeaves, std::vector<Ipv4InterfaceContainer>(NetworkTopology::nServersPerLeaf));
    
    InternetStackHelper internet;
    internet.SetRoutingHelper(ATPStaticRoutingHelper());
    internet.Install(nodes);
    
    NS_LOG_INFO("Assign IP Addresses.");
    Ipv4AddressHelper ipv4Helper;
    
    // 1. 连接Spine和Leaf
    for (uint32_t spineIdx = 0; spineIdx < NetworkTopology::nSpines; spineIdx++) {
        for (uint32_t leafIdx = 0; leafIdx < NetworkTopology::nLeaves; leafIdx++) {
            NodeContainer linkNodes(spineNodes.Get(spineIdx), leafNodes.Get(leafIdx));
            NetDeviceContainer devices = p2pSpineLeaf.Install(linkNodes);
            spineLeafDevices[spineIdx][leafIdx] = devices;
            
            std::ostringstream subnet;
            subnet << "10." << spineIdx + 1 << "." << leafIdx + 1 << ".0";
            ipv4Helper.SetBase(subnet.str().c_str(), "255.255.255.0");
            Ipv4InterfaceContainer interfaces = ipv4Helper.Assign(devices);
            spineLeafInterfaces[spineIdx][leafIdx] = interfaces;

            // 开启 ECN 并设置 256KB 阈值 (模拟真实交换机 buffer)
            Ptr<PointToPointNetDevice> spineDevice = DynamicCast<PointToPointNetDevice>(devices.Get(0));
            if (spineDevice != nullptr) {
                spineDevice->SetThreshold(60); // 设置相应的队列阈值
                spineDevice->SetEnableEcn(true);
            }
        }
    }
    
    // 2. 连接Leaf和Server
    for (uint32_t leafIdx = 0; leafIdx < NetworkTopology::nLeaves; leafIdx++) {
        for (uint32_t serverLocalIdx = 0; serverLocalIdx < NetworkTopology::nServersPerLeaf; serverLocalIdx++) {
            uint32_t globalServerIdx = leafIdx * NetworkTopology::nServersPerLeaf + serverLocalIdx;
            NodeContainer linkNodes(leafNodes.Get(leafIdx), serverNodes.Get(globalServerIdx));
            NetDeviceContainer devices = p2pLeafServer.Install(linkNodes);
            leafServerDevices[leafIdx][serverLocalIdx] = devices;
            
            std::ostringstream subnet;
            subnet << "192." << (leafIdx + 100) << "." << (serverLocalIdx + 1) * 10 << ".0";
            ipv4Helper.SetBase(subnet.str().c_str(), "255.255.255.0");
            Ipv4InterfaceContainer interfaces = ipv4Helper.Assign(devices);
            leafServerInterfaces[leafIdx][serverLocalIdx] = interfaces;

            Ptr<PointToPointNetDevice> leafDevice = DynamicCast<PointToPointNetDevice>(devices.Get(0)); // 交换机端
            if (leafDevice != nullptr) {
                leafDevice->SetThreshold(60); // Leaf 交换机端口阈值 (参考论文 100Gbps 推荐值)
                leafDevice->SetEnableEcn(true);
            }
        }
    }
    
    NS_LOG_INFO("Create Applications.");
    
    std::vector<Ptr<ATPPacketSink>> sinkApps;
    std::vector<Ptr<ATPSocket>> sinkSockets;
    std::vector<Address> sinkAddresses;
    std::vector<std::vector<Ptr<ATPBulkSendApplication>>> jobWorkerApps(jobs.size());
    std::vector<std::vector<Ptr<ATPSocket>>> jobWorkerSockets(jobs.size());

    for (size_t jobIdx = 0; jobIdx < jobs.size(); jobIdx++) {
        const auto& job = jobs[jobIdx];
        uint16_t sinkPort = 9;
        uint16_t sendPort = 11 + jobIdx;

        // ---------- 配置 Parameter Server (Sink) ----------
        uint32_t psNodeIndex = nodeNameToIndex[job.ps];
        uint32_t psLeafIndex = getServerLeafIndex(psNodeIndex);
        uint32_t psLocalIndex = getServerLocalIndex(psNodeIndex);
        
        Ptr<ATPPacketSink> sinkApp = CreateObject<ATPPacketSink>();
        Address sinkAddress(InetSocketAddress(leafServerInterfaces[psLeafIndex][psLocalIndex].GetAddress(1), sinkPort));
        sinkApp->SetAddressPort(sinkAddress, sinkPort);
        
        Ptr<Socket> sinkSocket = Socket::CreateSocket(nodes.Get(psNodeIndex), ATPSocketFactory::GetTypeId());
        Ptr<ATPSocket> sinkATPSocket = DynamicCast<ATPSocket>(sinkSocket);
        sinkApp->SetSocket(sinkATPSocket);
        sinkATPSocket->Bind(sinkAddress);
        sinkATPSocket->Listen();
        
        sinkApp->SetStartTime(Seconds(0.0));
        sinkApp->SetStopTime(stopTime);
        nodes.Get(psNodeIndex)->AddApplication(sinkApp);
        
        sinkApps.push_back(sinkApp);
        sinkSockets.push_back(sinkATPSocket);
        sinkAddresses.push_back(sinkAddress);
        
        // 绑定到运行时状态
        g_runtimeJobs[jobIdx].psSinkApp = sinkApp;

        // ---------- 配置 Tree & Workers ----------
        std::map<uint32_t, std::vector<std::string>> leafToWorkersMap;
        for (const auto& workerName : job.workers) {
            uint32_t wIdx = nodeNameToIndex[workerName];
            uint32_t lIdx = getServerLeafIndex(wIdx);
            leafToWorkersMap[lIdx].push_back(workerName);
        }

        JobTree tree(job.id);
        tree.fanInDegree1 = leafToWorkersMap.size();
        tree.fullBitmap1 = (1 << leafToWorkersMap.size()) - 1;

        int leafLogicalIdx = 0;
        uint32_t flatBitmap = 0;
        int globalWorkerCount = 0;
        std::map<uint32_t, int> leafLogicalIndexMap;

        for (auto& entry : leafToWorkersMap) {
            uint32_t count = entry.second.size();
            tree.AddBranch(leafLogicalIdx, count, (1 << count) - 1);
            for (size_t k = 0; k < count; ++k) flatBitmap |= (1 << (globalWorkerCount + k));
            globalWorkerCount += count;
            leafLogicalIndexMap[entry.first] = leafLogicalIdx;
            leafLogicalIdx++;
        }
        tree.expectedFlatBitmap = flatBitmap;
        sinkSockets[jobIdx]->SetJobTree(tree);

        Address workerAddress(InetSocketAddress(Ipv4Address::GetAny(), sendPort));

        for (const auto& workerName : job.workers) {
            uint32_t workerNodeIndex = nodeNameToIndex[workerName];
            uint32_t workerLeafIndex = getServerLeafIndex(workerNodeIndex);

            Ptr<ATPBulkSendApplication> workerApp = CreateObject<ATPBulkSendApplication>();
            Ptr<Socket> workerSocket = Socket::CreateSocket(nodes.Get(workerNodeIndex), ATPSocketFactory::GetTypeId());
            Ptr<ATPSocket> worker_ATPSocket = DynamicCast<ATPSocket>(workerSocket);

            worker_ATPSocket->SetInitCwnd(initCwnd);
            worker_ATPSocket->SetRetxTimeout(MicroSeconds(1000)); // 基础 RTO 1ms
            worker_ATPSocket->SetRetxCheckInterval(MicroSeconds(200)); // 每 200us 轮询一次检查丢包

            // 修改：使用 0 作为这一轮要发送的极限
            workerApp->Setup(sinkAddresses[jobIdx], worker_ATPSocket, 0, job.id);
            workerApp->SetEnableATPTag(true);
            workerApp->SetStartTime(Seconds(1.0)); // 所有第一轮训练统一从 1.0s 起跑
            workerApp->SetStopTime(stopTime);

            const auto& localWorkers = leafToWorkersMap[workerLeafIndex];
            int myIdxInLeaf = -1;
            for (size_t i = 0; i < localWorkers.size(); ++i) {
                if (localWorkers[i] == workerName) { myIdxInLeaf = i; break; }
            }

            workerApp->SetFaninDegree0(localWorkers.size());
            workerApp->SetBitmap0(1 << myIdxInLeaf);
            workerApp->SetFaninDegree1(leafToWorkersMap.size());
            workerApp->SetBitmap1(1 << leafLogicalIndexMap[workerLeafIndex]);

            worker_ATPSocket->SetConnectCallback(
                MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, workerApp),
                MakeCallback(&ATPBulkSendApplication::ConnectionFailed, workerApp));
            worker_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, workerApp));
            worker_ATPSocket->Bind(workerAddress);
            worker_ATPSocket->Connect(sinkAddresses[jobIdx]);

            nodes.Get(workerNodeIndex)->AddApplication(workerApp);

            jobWorkerApps[jobIdx].push_back(workerApp);
            jobWorkerSockets[jobIdx].push_back(worker_ATPSocket);
            
            // 绑定到运行时状态
            g_runtimeJobs[jobIdx].workerApps.push_back(workerApp);

            if (jobWorkerApps[jobIdx].size() == 1) {
                if (jobIdx == 0) worker_ATPSocket->GetTxBuffer()->TraceConnectWithoutContext("cwndTrace", MakeCallback(&CwndChange_job1));
                if (jobIdx == 2) worker_ATPSocket->GetTxBuffer()->TraceConnectWithoutContext("cwndTrace", MakeCallback(&CwndChange_job2));
            }
        }
        sinkSockets[jobIdx]->AddAddressMapping(job.id, GenerateMulticastAddress(job.id), sendPort);
    }
    
    NS_LOG_INFO("Configure routing.");
    // ------------- 静态路由与多播配置 -------------
    ATPStaticRoutingHelper staticRoutingHelper;
    std::vector<Ptr<ATPStaticRouting>> spineRouting(NetworkTopology::nSpines);
    std::vector<Ptr<ATPStaticRouting>> leafRouting(NetworkTopology::nLeaves);
    std::vector<Ptr<ATPStaticRouting>> serverRouting(NetworkTopology::nTotalServers);
    
    for (uint32_t i = 0; i < NetworkTopology::nSpines; i++) {
        spineRouting[i] = staticRoutingHelper.GetStaticRouting(spineNodes.Get(i)->GetObject<Ipv4>());
        spineRouting[i]->SetEnableAggregation(false);
    }
    for (uint32_t i = 0; i < NetworkTopology::nLeaves; i++) {
        leafRouting[i] = staticRoutingHelper.GetStaticRouting(leafNodes.Get(i)->GetObject<Ipv4>());
        leafRouting[i]->SetEnableAggregation(true);
    }
    for (uint32_t i = 0; i < NetworkTopology::nTotalServers; i++) {
        serverRouting[i] = staticRoutingHelper.GetStaticRouting(serverNodes.Get(i)->GetObject<Ipv4>());
    }
    
    // Server 路由：明确去往所有其他 Server 的路径 (发给直连的 Leaf)
    for (uint32_t serverIdx = 0; serverIdx < NetworkTopology::nTotalServers; serverIdx++)
    {
        uint32_t leafIndex = getServerLeafIndex(NetworkTopology::serverStartIdx + serverIdx);
        uint32_t localIndex = getServerLocalIndex(NetworkTopology::serverStartIdx + serverIdx);
        Ipv4Address leafGateway = leafServerInterfaces[leafIndex][localIndex].GetAddress(0);

        for (uint32_t targetServerIdx = 0; targetServerIdx < NetworkTopology::nTotalServers;
             targetServerIdx++)
        {
            if (targetServerIdx != serverIdx)
            {
                uint32_t targetLeafIndex =
                    getServerLeafIndex(NetworkTopology::serverStartIdx + targetServerIdx);
                uint32_t targetLocalIndex =
                    getServerLocalIndex(NetworkTopology::serverStartIdx + targetServerIdx);
                Ipv4Address targetAddress =
                    leafServerInterfaces[targetLeafIndex][targetLocalIndex].GetAddress(1);
                serverRouting[serverIdx]->AddHostRouteTo(targetAddress, leafGateway, 1);
            }
        }
    }

    // ARO 路由覆盖：强制重写跨机架上行流量的 Spine 路径
    for (const auto& jobRouteEntry : routeConfig.jobRoutes)
    {
        uint32_t jobId = jobRouteEntry.first;
        if (jobId > jobs.size() || jobId == 0)
            continue;
        const auto& targetJob = jobs[jobId - 1];

        uint32_t psNodeIdx = nodeNameToIndex[targetJob.ps];
        uint32_t psLeafIdx = getServerLeafIndex(psNodeIdx);
        uint32_t psLocalIdx = getServerLocalIndex(psNodeIdx);
        Ipv4Address psIp = leafServerInterfaces[psLeafIdx][psLocalIdx].GetAddress(1);

        for (const auto& routePair : jobRouteEntry.second)
        {
            uint32_t fromIndex = nodeNameToIndex[routePair.first];
            uint32_t toIndex = nodeNameToIndex[routePair.second];

            if (fromIndex >= NetworkTopology::leafStartIdx &&
                fromIndex < NetworkTopology::serverStartIdx)
            {
                uint32_t leafIdx = fromIndex - NetworkTopology::leafStartIdx;
                // 让同机架的包在本地直连路由给 PS
                if (leafIdx == psLeafIdx)
                {
                    continue;
                }
                if (toIndex >= NetworkTopology::spineStartIdx &&
                    toIndex < NetworkTopology::leafStartIdx)
                {
                    uint32_t spineIdx = toIndex - NetworkTopology::spineStartIdx;
                    Ipv4Address nextHop = spineLeafInterfaces[spineIdx][leafIdx].GetAddress(0);
                    // 【覆盖默认路由】前往 PS 的包，强制走 ARO 分配的 Spine
                    leafRouting[leafIdx]->AddHostRouteTo(psIp, nextHop, spineIdx + 1);
                }
            }
        }
    }

    // Leaf 路由：分为直连、远端和 ARO 覆盖
    for (uint32_t leafIdx = 0; leafIdx < NetworkTopology::nLeaves; leafIdx++)
    {
        // A. 本地直连 Server
        for (uint32_t serverLocalIdx = 0; serverLocalIdx < NetworkTopology::nServersPerLeaf;
             serverLocalIdx++)
        {
            Ipv4Address serverAddress = leafServerInterfaces[leafIdx][serverLocalIdx].GetAddress(1);
            // Leaf 连向 Server 的端口编号是 nSpines + serverLocalIdx + 1
            leafRouting[leafIdx]->AddHostRouteTo(serverAddress,
                                                 serverAddress,
                                                 NetworkTopology::nSpines + serverLocalIdx + 1);
        }

        // B. 远端 Server
        for (uint32_t targetLeafIdx = 0; targetLeafIdx < NetworkTopology::nLeaves; targetLeafIdx++)
        {
            uint32_t spineChoice = targetLeafIdx % NetworkTopology::nSpines;
            Ipv4Address spineGateway = spineLeafInterfaces[spineChoice][leafIdx].GetAddress(0);
            if (targetLeafIdx != leafIdx)
            {
                for (uint32_t serverLocalIdx = 0; serverLocalIdx < NetworkTopology::nServersPerLeaf;
                     serverLocalIdx++)
                {
                    Ipv4Address targetAddress =
                        leafServerInterfaces[targetLeafIdx][serverLocalIdx].GetAddress(1);
                    leafRouting[leafIdx]->AddHostRouteTo(targetAddress,
                                                         spineGateway,
                                                         spineChoice + 1); // 连向 Spine 的端口
                }
            }
        }
    }

    // Spine 路由：明确下行去往所有 Server 的路径
    for (uint32_t spineIdx = 0; spineIdx < NetworkTopology::nSpines; spineIdx++)
    {
        for (uint32_t leafIdx = 0; leafIdx < NetworkTopology::nLeaves; leafIdx++)
        {
            Ipv4Address leafGateway = spineLeafInterfaces[spineIdx][leafIdx].GetAddress(1);
            for (uint32_t serverLocalIdx = 0; serverLocalIdx < NetworkTopology::nServersPerLeaf;
                 serverLocalIdx++)
            {
                Ipv4Address serverAddress =
                    leafServerInterfaces[leafIdx][serverLocalIdx].GetAddress(1);
                spineRouting[spineIdx]->AddHostRouteTo(serverAddress, leafGateway, leafIdx + 1);
            }
        }
    }

    // 配置下行多播路由
    for (size_t jobIdx = 0; jobIdx < jobs.size(); ++jobIdx) {
        const auto& job = jobs[jobIdx];
        uint32_t jobId = job.id;
        Ipv4Address multicastGroup = GenerateMulticastAddress(jobId);

        uint32_t psNodeIdx = nodeNameToIndex[job.ps];
        uint32_t psLeafIdx = getServerLeafIndex(psNodeIdx);
        uint32_t psLocalIdx = getServerLocalIndex(psNodeIdx);
        Ipv4Address psAddress = leafServerInterfaces[psLeafIdx][psLocalIdx].GetAddress(1);

        std::map<uint32_t, std::vector<uint32_t>> spineToLeafsMap;
        std::set<uint32_t> activeSpines;

        if (routeConfig.jobRoutes.count(jobId)) {
            for (const auto& routePair : routeConfig.jobRoutes.at(jobId)) {
                uint32_t lIdx = nodeNameToIndex[routePair.first] - NetworkTopology::leafStartIdx;
                uint32_t sIdx = nodeNameToIndex[routePair.second] - NetworkTopology::spineStartIdx;
                spineToLeafsMap[sIdx].push_back(lIdx);
                activeSpines.insert(sIdx);
            }
        }

        std::map<uint32_t, std::vector<uint32_t>> leafToWorkerInterfaces;
        for (const auto& workerName : job.workers) {
            uint32_t wIdx = nodeNameToIndex[workerName];
            uint32_t wLeafIdx = getServerLeafIndex(wIdx);
            uint32_t wLocalIdx = getServerLocalIndex(wIdx);
            leafToWorkerInterfaces[wLeafIdx].push_back(NetworkTopology::nSpines + wLocalIdx + 1);
        }

        serverRouting[psNodeIdx - NetworkTopology::serverStartIdx]->SetDefaultMulticastRoute(1);

        {
            uint32_t inputFromPS = NetworkTopology::nSpines + psLocalIdx + 1;
            std::vector<uint32_t> outputInterfaces;
            for (uint32_t sIdx : activeSpines) outputInterfaces.push_back(sIdx + 1);
            if (leafToWorkerInterfaces.count(psLeafIdx)) {
                for (uint32_t ifIdx : leafToWorkerInterfaces[psLeafIdx]) outputInterfaces.push_back(ifIdx);
            }
            leafRouting[psLeafIdx]->AddMulticastRoute(psAddress, multicastGroup, inputFromPS, outputInterfaces);
        }

        for (uint32_t sIdx : activeSpines) {
            std::vector<uint32_t> outputToTargetLeafs;
            for (uint32_t targetLeafIdx : spineToLeafsMap[sIdx]) {
                if (targetLeafIdx != psLeafIdx) outputToTargetLeafs.push_back(targetLeafIdx + 1);
            }
            if (!outputToTargetLeafs.empty()) {
                spineRouting[sIdx]->AddMulticastRoute(psAddress, multicastGroup, psLeafIdx + 1, outputToTargetLeafs);
            }
        }

        for (const auto& entry : spineToLeafsMap) {
            uint32_t sIdx = entry.first;
            for (uint32_t leafIdx : entry.second) {
                if (leafIdx == psLeafIdx || leafToWorkerInterfaces.find(leafIdx) == leafToWorkerInterfaces.end()) continue;
                leafRouting[leafIdx]->AddMulticastRoute(psAddress, multicastGroup, sIdx + 1, leafToWorkerInterfaces[leafIdx]);
            }
        }
    }

    // 启动全局迭代监控器：在第一轮发包开始后 1 毫秒启动监控
    for (const auto& job : g_runtimeJobs) {
        Simulator::Schedule(Seconds(1.0) + MilliSeconds(1), &DmlJobCoordinator::CheckIterationProgress, job.id);
    }
    if (sinkApps.size() >= 3)
    {
        Simulator::Schedule(Seconds(1.0), &Measurement, sinkApps[0], sinkApps[2]);
    }
    
    NS_LOG_INFO("Run Simulation.");
    Simulator::Stop(stopTime);
    Simulator::Run();
    Simulator::Destroy();
    
    cwndStream_job1.close();
    cwndStream_job2.close();
    SinkBytesStream_job1.close();
    SinkBytesStream_job2.close();

    NS_LOG_INFO("Done.");
    return 0;
}
