/*
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * ATP 参数化实验：Leaf-Spine 拓扑
 */

#include "ns3/applications-module.h"
#include "ns3/atp-module.h"
#include "ns3/core-module.h"
#include "ns3/drop-tail-queue.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include "ns3/packet-sink.h"
#include "ns3/point-to-point-module.h"
#include "ns3/ptr.h"
#include "ns3/queue.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

using namespace ns3;
using namespace std;

NS_LOG_COMPONENT_DEFINE("Main-Experiment");

// ============================================================
// 全局输出流与状态
// ============================================================

/// @brief 各 Job 拥塞窗口跟踪输出流，索引对应 Job ID-1
static std::vector<std::ofstream> g_cwndStreams;

/// @brief 各 Job 吞吐量采样输出流
static std::vector<std::ofstream> g_sinkBytesStreams;

/// @brief 交换机队列长度采样输出流
static std::ofstream g_queueStream;

/// @brief 实验汇总统计输出流
static std::ofstream g_summaryStream;

/// @brief PS 端 ATPPacketSink 应用句柄，用于读取各 Job 总接收字节
static std::vector<Ptr<ATPPacketSink>> g_sinkApps;

/// @brief 上次采样时各 Job 已接收字节数，用于计算增量
static std::vector<uint64_t> g_lastJobBytes;

/// @brief 仿真开始时间戳（微秒），用于输出对齐
static uint64_t g_startTimeUs = 0;

/// @brief 所有 Job 是否已完成，避免重复调用 Simulator::Stop
static bool g_allJobsCompleted = false;

/// @brief 仿真实际启动时间，用于准确计算吞吐量
static Time g_simStartTime;

// ============================================================
// 命令行参数
// ============================================================

// --- DML 实验参数 ---
static uint32_t g_numJobs = 8;              ///< Job 总数
static uint32_t g_workersPerJob = 12;       ///< 每 Job Worker 数（需 ≤32，受位图限制）
static uint32_t g_switchMemoryKB = 256;     ///< 交换机队列缓存（KB）
static uint32_t g_packetSize = 300;         ///< 包长（字节）
static uint32_t g_stragglerDegree = 0;      ///< 慢节点程度：0=无，1=轻度(0.5x)，2=严重(0.25x)
static std::string g_dnnModel = "resnet50"; ///< 模型名称，决定模型大小与计算时间
static double g_linkDelayUs = 2.0;          ///< 链路传播延迟（微秒）
static uint32_t g_durationUs = 20000;       ///< 仿真时长（微秒）
static std::string g_outputDir = "atp-result/experiment"; ///< 结果输出根目录
static uint32_t g_initCwnd = 32;                          ///< ATP 初始拥塞窗口（包）
static bool g_enableEcn = true;                           ///< 是否启用 ECN 标记
static uint32_t g_spineEcnThresholdPkts = 100;            ///< Spine 交换机 ECN 标记阈值（队列包数）
static uint32_t g_leafEcnThresholdPkts = 100;             ///< Leaf 交换机 ECN 标记阈值（队列包数）
static uint32_t g_maxIterations = 5;                      ///< 每 Job 训练迭代轮数
static uint32_t g_seed = 42;                              ///< 拓扑分配随机种子

// --- 拓扑维度参数 ---
static uint32_t g_nSpines = 8;          ///< Spine 交换机数量
static uint32_t g_nLeaves = 8;          ///< Leaf 交换机数量
static uint32_t g_nServersPerLeaf = 16; ///< 每 Leaf 下挂服务器数量

// --- 拓扑生成模式 ---
static std::string g_topoMode = "auto";    ///< 拓扑模式: "auto" 自动贪心生成, "json" 读取 JSON 配置
static std::string g_jobsConfigFile = "";  ///< JSON jobs 配置文件路径（json 模式）
static std::string g_routeConfigFile = ""; ///< JSON routes 配置文件路径（json 模式）

// --- 背景流量参数 ---
static bool g_bgEnable = false;              ///< 是否启用背景 UDP 流
static std::string g_bgDataRate = "30Gbps";  ///< 背景流发送速率
static uint32_t g_bgPacketSize = 1500;       ///< 背景流包长（字节）
static double g_bgMeanInterval = 0.0002;     ///< 背景流到达间隔（秒，泊松）
static uint32_t g_bgMaxFlows = 400000;       ///< 背景流最大数量上限
static double g_bgStartTime = 1.0;           ///< 背景流启动时间（秒）
static double g_bgMouseProb = 0.8;           ///< 老鼠流概率（小流）
static uint32_t g_bgMouseBytes = 50000;      ///< 老鼠流大小（字节）
static uint32_t g_bgElephantBytes = 1000000; ///< 大象流大小（字节）

// ============================================================
// 模型配置
// ============================================================

/**
 * @struct DnnProfile
 * @brief 预定义 DNN 模型的参数轮廓
 */
struct DnnProfile
{
    std::string name;        ///< 模型名称
    uint64_t modelSizeBytes; ///< 单轮迭代需传输的梯度/模型字节数
    Time computeTime;        ///< PS 端计算耗时（用于迭代间隔调度）
};

/// @brief 根据模型名称返回对应参数轮廓
static DnnProfile
GetDnnProfile(const std::string& name)
{
    if (name == "resnet50")
        return {"ResNet50", 98ULL * 1024 * 1024, MilliSeconds(50)};
    if (name == "alexnet")
        return {"AlexNet", 233ULL * 1024 * 1024, MilliSeconds(30)};
    if (name == "vgg19")
        return {"VGG19", 548ULL * 1024 * 1024, MilliSeconds(150)};
    return {"BertBase", 440ULL * 1024 * 1024, MilliSeconds(180)};
}

// ============================================================
// 迭代运行时状态
// ============================================================

/**
 * @struct DmlJobRuntime
 * @brief 单个 Job 在仿真运行时的动态状态
 */
struct DmlJobRuntime
{
    uint32_t id;                                         ///< Job ID（从 1 开始）
    std::string modelName;                               ///< 模型名称
    uint64_t modelSizeBytes;                             ///< 模型字节数
    Time computeTime;                                    ///< 计算等待时间
    uint32_t currentIteration;                           ///< 当前已完成迭代数
    uint32_t maxIterations;                              ///< 目标迭代数
    Ptr<ATPPacketSink> psSinkApp;                        ///< PS 接收端应用
    std::vector<Ptr<ATPBulkSendApplication>> workerApps; ///< 各 Worker 发送端应用
    Time startTime;                                      ///< Job 实际开始通信时间
    Time endTime;                                        ///< Job 完成全部迭代时间（未完成时 = 0）
};

/// @brief 全局 Job 运行时状态表，长度 = g_numJobs
static std::vector<DmlJobRuntime> g_runtimeJobs;

// ============================================================
// 拓扑配置结果
// ============================================================

/**
 * @struct TopoConfig
 * @brief Leaf-Spine 拓扑中 Job 节点分配与路由元数据聚合
 */
struct TopoConfig
{
    /// @brief Job j 的 PS 节点全局索引，长度 = g_numJobs
    std::vector<uint32_t> psNodeIndices;

    /// @brief Job j 的所有 Worker 节点全局索引，[j][w] 为第 w 个 Worker
    std::vector<std::vector<uint32_t>> workerNodeIndices;

    /// @brief Job j 关联的 Leaf 列表；[0] 为 PS 所在 Leaf，[1..k] 为 Worker Leaf
    std::vector<std::vector<uint32_t>> jobLeaves;

    /// @brief Job 通信路径的 (leafIdx, spineIdx) 列表，用于 ARO 与组播路由配置
    std::map<uint32_t, std::vector<std::pair<uint32_t, uint32_t>>> jobRoutes;

    /// @brief 被标记为 straggler 的服务器节点全局索引集合
    std::set<uint32_t> stragglerNodes;

    /// @brief Job j 中被指定为 straggler 的 Worker 局部索引；UINT32_MAX 表示无
    std::vector<uint32_t> jobStragglerWorkerIdx;

    /// @brief 每 Leaf 分配给同一 Job 的 Worker 数量（块大小）
    uint32_t chunkSize = 0;

    /// @brief 每个 Job 的 Worker 分布在多少个不同 Leaf 上（k = workersPerJob / chunkSize）
    uint32_t k = 0;

    /// @brief 每个 Leaf 平均关联的 Job 数量，用于评估 Leaf 级负载均衡
    uint32_t nAssocJobs = 0;
};

// ============================================================
// 辅助函数
// ============================================================

/// @brief 递归创建目录（兼容 Windows / Linux）
static void
EnsureDir(const std::string& path)
{
#ifdef _WIN32
    mkdir(path.c_str());
#else
    mkdir(path.c_str(), 0755);
#endif
}

/// @brief 由服务器全局索引计算所属 Leaf 编号
/// @param serverStartIdx 服务器节点在全局 NodeContainer 中的起始偏移
static uint32_t
getServerLeafIndex(uint32_t serverIndex, uint32_t serverStartIdx)
{
    return (serverIndex - serverStartIdx) / g_nServersPerLeaf;
}

/// @brief 由服务器全局索引计算在 Leaf 内的局部偏移
static uint32_t
getServerLocalIndex(uint32_t serverIndex, uint32_t serverStartIdx)
{
    return (serverIndex - serverStartIdx) % g_nServersPerLeaf;
}

/// @brief 为指定 Job 生成组播地址（225.1.jobId.1）
static Ipv4Address
GenerateMulticastAddress(uint32_t jobId)
{
    return Ipv4Address((225U << 24) | (1U << 16) | (jobId << 8) | 1U);
}

// ============================================================
// JSON 解析辅助模块
// ============================================================

/**
 * @struct JobConfig
 * @brief 从 JSON jobs 配置文件解析的单个 Job 静态配置
 */
struct JobConfig
{
    uint32_t id;                      ///< Job ID
    std::string model;                ///< 模型名称
    std::vector<std::string> workers; ///< Worker 服务器名称列表
    std::string ps;                   ///< PS 服务器名称
    std::string ps_leaf;              ///< PS 所在 Leaf 名称
};

/**
 * @struct RouteConfig
 * @brief 从 JSON routes 配置文件解析的路由配置
 */
struct RouteConfig
{
    std::map<uint32_t, std::vector<std::pair<std::string, std::string>>> jobRoutes;
};

/// @brief 去除字符串首尾空白
static std::string
trim(const std::string& str)
{
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos)
        return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, (last - first + 1));
}

/// @brief 去除字符串首尾引号
static std::string
stripQuotes(const std::string& str)
{
    if (str.size() >= 2 && str.front() == '"' && str.back() == '"')
    {
        return str.substr(1, str.size() - 2);
    }
    return str;
}

/// @brief 解析 JSON 字符串数组，如 [ "a", "b" ]
static std::vector<std::string>
parseStringArray(const std::string& arrayStr)
{
    std::vector<std::string> result;
    std::string content = arrayStr;
    size_t start = content.find('[');
    size_t end = content.find_last_of(']');
    if (start != std::string::npos && end != std::string::npos)
    {
        content = content.substr(start + 1, end - start - 1);
    }
    std::stringstream ss(content);
    std::string item;
    while (std::getline(ss, item, ','))
    {
        item = trim(item);
        item = stripQuotes(item);
        if (!item.empty())
            result.push_back(item);
    }
    return result;
}

/// @brief 从服务器名称（如 "server25"）提取全局节点索引
static uint32_t
ServerNameToIndex(const std::string& name, uint32_t serverStartIdx)
{
    if (name.find("server") == 0)
    {
        uint32_t num = std::stoi(name.substr(6));
        return serverStartIdx + (num - 1);
    }
    NS_FATAL_ERROR("Invalid server name: " << name);
    return 0;
}

/// @brief 从 Leaf 名称（如 "leaf4"）提取 0-based 索引
static uint32_t
LeafNameToIndex(const std::string& name)
{
    if (name.find("leaf") == 0)
    {
        return std::stoi(name.substr(4)) - 1;
    }
    NS_FATAL_ERROR("Invalid leaf name: " << name);
    return 0;
}

/// @brief 从 Spine 名称（如 "spine1"）提取 0-based 索引
static uint32_t
SpineNameToIndex(const std::string& name)
{
    if (name.find("spine") == 0)
    {
        return std::stoi(name.substr(5)) - 1;
    }
    NS_FATAL_ERROR("Invalid spine name: " << name);
    return 0;
}

/**
 * @brief 解析 JSON jobs 配置文件（对象数组格式）
 */
static std::vector<JobConfig>
ParseJobsConfig(const std::string& filename)
{
    std::vector<JobConfig> jobs;
    std::ifstream file(filename);
    if (!file.is_open())
        NS_FATAL_ERROR("Cannot open jobs config file: " << filename);

    std::string line;
    JobConfig currentJob;
    bool inJob = false;
    bool inWorkers = false;
    std::string workersContent;

    while (std::getline(file, line))
    {
        line = trim(line);
        if (line.empty())
            continue;

        if (line == "{" && !inJob)
        {
            inJob = true;
            currentJob = JobConfig();
            continue;
        }

        if ((line == "}" || line == "},") && inJob)
        {
            jobs.push_back(currentJob);
            currentJob = JobConfig();
            inJob = false;
            inWorkers = false;
            workersContent.clear();
            continue;
        }

        if (line.find("\"id\":") != std::string::npos)
        {
            size_t pos = line.find(':');
            if (pos != std::string::npos)
            {
                std::string idStr = trim(line.substr(pos + 1));
                if (!idStr.empty() && idStr.back() == ',')
                    idStr.pop_back();
                currentJob.id = std::stoi(stripQuotes(idStr));
            }
        }
        else if (line.find("\"model\":") != std::string::npos)
        {
            size_t pos = line.find(':');
            if (pos != std::string::npos)
            {
                std::string modelStr = trim(line.substr(pos + 1));
                if (!modelStr.empty() && modelStr.back() == ',')
                    modelStr.pop_back();
                currentJob.model = stripQuotes(modelStr);
            }
        }
        else if (line.find("\"workers\":") != std::string::npos)
        {
            inWorkers = true;
            size_t pos = line.find('[');
            if (pos != std::string::npos)
                workersContent = line.substr(pos);
            else
                workersContent = "[";

            // 同一行即闭合
            if (line.find(']') != std::string::npos)
            {
                currentJob.workers = parseStringArray(workersContent);
                inWorkers = false;
                workersContent.clear();
            }
        }
        else if (inWorkers && line.find(']') != std::string::npos)
        {
            if (workersContent.find(']') == std::string::npos)
                workersContent += line;
            currentJob.workers = parseStringArray(workersContent);
            inWorkers = false;
            workersContent.clear();
        }
        else if (inWorkers)
        {
            workersContent += line;
        }
        else if (line.find("\"ps\":") != std::string::npos)
        {
            size_t pos = line.find(':');
            if (pos != std::string::npos)
            {
                std::string psStr = trim(line.substr(pos + 1));
                if (!psStr.empty() && psStr.back() == ',')
                    psStr.pop_back();
                currentJob.ps = stripQuotes(psStr);
            }
        }
        else if (line.find("\"ps_leaf\":") != std::string::npos)
        {
            size_t pos = line.find(':');
            if (pos != std::string::npos)
            {
                std::string psLeafStr = trim(line.substr(pos + 1));
                if (!psLeafStr.empty() && psLeafStr.back() == ',')
                    psLeafStr.pop_back();
                currentJob.ps_leaf = stripQuotes(psLeafStr);
            }
        }
    }
    return jobs;
}

/**
 * @brief 解析 JSON routes 配置文件（含 "jobRoutes" 外层）
 */
static RouteConfig
ParseRouteConfig(const std::string& filename)
{
    RouteConfig config;
    std::ifstream file(filename);
    if (!file.is_open())
        NS_FATAL_ERROR("Cannot open route config file: " << filename);

    std::string line;
    uint32_t currentJobId = 0;
    bool inJobRoutes = false;

    while (std::getline(file, line))
    {
        line = trim(line);
        if (line.empty())
            continue;

        if (line.find("\"jobRoutes\"") != std::string::npos)
        {
            inJobRoutes = true;
            continue;
        }

        if (!inJobRoutes)
            continue;

        for (uint32_t i = 1; i <= g_numJobs; ++i)
        {
            std::string key = "\"" + std::to_string(i) + "\"";
            if (line.find(key) != std::string::npos)
            {
                currentJobId = i;
                break;
            }
        }

        if (currentJobId > 0 && line.find('[') != std::string::npos &&
            line.find("leaf") != std::string::npos)
        {
            size_t start = line.find('[');
            size_t end = line.find(']', start);
            if (start != std::string::npos && end != std::string::npos)
            {
                std::string routeStr = line.substr(start, end - start + 1);
                auto pair = parseStringArray(routeStr);
                if (pair.size() == 2)
                {
                    config.jobRoutes[currentJobId].push_back({pair[0], pair[1]});
                }
            }
        }

        if ((line == "}," || line == "}") && currentJobId > 0)
        {
            currentJobId = 0;
        }
    }
    return config;
}

// ============================================================
// 测量回调
// ============================================================

/// @brief Job 拥塞窗口变化回调，写入对应 cwnd 跟踪文件
static void
CwndChange(uint32_t jobIdx, uint32_t oldCwnd, uint32_t newCwnd)
{
    if (jobIdx < g_cwndStreams.size() && g_cwndStreams[jobIdx].is_open())
    {
        g_cwndStreams[jobIdx] << Simulator::Now().GetMicroSeconds() - g_startTimeUs << "\t"
                              << newCwnd << std::endl;
    }
}

/// @brief 周期性吞吐量采样回调（每 100μs），统计各 Job PS 接收字节增量
static void
Measurement(uint32_t numJobs)
{
    Time now = Simulator::Now();
    for (uint32_t j = 0; j < numJobs; ++j)
    {
        uint64_t current = g_sinkApps[j]->GetTotalRxJob(j + 1);
        uint64_t delta = current - g_lastJobBytes[j];
        if (j < g_sinkBytesStreams.size() && g_sinkBytesStreams[j].is_open())
        {
            g_sinkBytesStreams[j] << now.GetMicroSeconds() - g_startTimeUs << "\t" << current
                                  << "\t" << delta << std::endl;
        }
        g_lastJobBytes[j] = current;
    }
    Simulator::Schedule(MicroSeconds(100), &Measurement, numJobs);
}

/// @brief 周期性队列长度采样回调（每 10μs），针对指定交换机端口
static void
SampleQueue(Ptr<PointToPointNetDevice> device)
{
    if (g_queueStream.is_open())
    {
        uint32_t qSize = device->GetQueue()->GetNPackets();
        g_queueStream << Simulator::Now().GetMicroSeconds() - g_startTimeUs << "\t" << qSize
                      << std::endl;
    }
    Simulator::Schedule(MicroSeconds(10), &SampleQueue, device);
}

// ============================================================
// 迭代控制器
// ============================================================

/**
 * @class DmlJobCoordinator
 * @brief 调度 Job 的迭代推进：检测 PS 收齐本轮梯度后触发计算阶段，再进入下一轮通信
 */
class DmlJobCoordinator
{
  public:
    /// @brief 检查 Job 当前迭代是否已收齐 modelSizeBytes 数据，若收齐则推进迭代
    static void CheckIterationProgress(uint32_t jobId)
    {
        DmlJobRuntime& job = g_runtimeJobs[jobId - 1];
        if (job.currentIteration >= job.maxIterations)
            return;

        uint64_t currentRx = job.psSinkApp->GetTotalRxJob(job.id);
        uint64_t targetRx = job.modelSizeBytes * (job.currentIteration + 1);

        if (currentRx >= targetRx)
        {
            // 收齐本轮：暂停各 Worker 发送，进入计算阶段
            for (auto& workerApp : job.workerApps)
            {
                workerApp->SetMaxBytes(currentRx);
            }
            job.currentIteration++;
            std::cout << "Time: " << std::fixed << std::setprecision(3)
                      << Simulator::Now().GetSeconds() << "s | Job " << job.id << " "
                      << job.modelName << " 完成迭代 " << job.currentIteration << "/"
                      << job.maxIterations << std::endl;

            if (job.currentIteration < job.maxIterations)
            {
                // 计算完成后重新开放通信
                Simulator::Schedule(job.computeTime,
                                    &DmlJobCoordinator::StartCommunicationPhase,
                                    job.id);
            }
            else
            {
                job.endTime = Simulator::Now();
                std::cout << ">>> Job " << job.id << " 完成全部训练迭代 @ " << std::fixed
                          << std::setprecision(6) << job.endTime.GetSeconds() << "s" << std::endl;

                bool allDone = true;
                for (const auto& rj : g_runtimeJobs)
                {
                    if (rj.currentIteration < rj.maxIterations)
                    {
                        allDone = false;
                        break;
                    }
                }
                if (allDone && !g_allJobsCompleted)
                {
                    g_allJobsCompleted = true;
                    Time now = Simulator::Now();
                    std::cout << ">>> 所有 Job 已完成，提前终止仿真 @ " << std::fixed
                              << std::setprecision(6) << now.GetSeconds() << "s" << std::endl;
                    Simulator::Stop(now);
                }
            }
        }
        else
        {
            // 未收齐：1ms 后再次检查
            Simulator::Schedule(MilliSeconds(1), &DmlJobCoordinator::CheckIterationProgress, jobId);
        }
    }

    /// @brief 开放 Worker 发送限制，启动新一轮通信阶段
    static void StartCommunicationPhase(uint32_t jobId)
    {
        DmlJobRuntime& job = g_runtimeJobs[jobId - 1];
        for (auto& workerApp : job.workerApps)
        {
            workerApp->SetMaxBytes(0); // 0 表示无上限
        }
        Simulator::Schedule(MilliSeconds(1), &DmlJobCoordinator::CheckIterationProgress, jobId);
    }
};

// ============================================================
// 模块 1a：自动拓扑生成（贪心负载均衡 + 轮询 Spine 分配）
// ============================================================

/**
 * @brief 自动生成满足约束的拓扑分配方案
 * @param serverStartIdx 服务器节点在全局 NodeContainer 中的起始偏移
 * @return TopoConfig 包含 PS/Worker 节点索引、Leaf 关联、路由元数据
 *
 * 约束：
 * 1. Worker 不与 PS 同 Leaf；
 * 2. 每 Leaf 上同一 Job 的 Worker 连续紧凑排列（chunkSize）；
 * 3. 各 Leaf 关联 Job 数尽可能均衡；
 * 4. 同一 Job 的所有 Worker Leaf 共享同一 Spine，按 Job ID 轮询分配。
 */
static TopoConfig
GenerateTopologyAuto(uint32_t serverStartIdx)
{
    TopoConfig topo;
    topo.psNodeIndices.resize(g_numJobs);
    topo.workerNodeIndices.assign(g_numJobs, std::vector<uint32_t>(g_workersPerJob));
    topo.jobStragglerWorkerIdx.assign(g_numJobs, UINT32_MAX);
    topo.jobLeaves.resize(g_numJobs);

    std::mt19937 rng(g_seed);

    uint32_t m = 4;
    topo.chunkSize = m;
    topo.k = g_workersPerJob / m;
    topo.nAssocJobs = g_numJobs * (1 + topo.k) / g_nLeaves;

    // --- 2. 构造 Job-Leaf 关联（PS 与 Worker 分离）---
    std::vector<uint32_t> leafAssocCount(g_nLeaves, 0);

    // 2a. 为每个 Job 选择 PS 所在 Leaf（取当前负载最轻）
    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        uint32_t bestLeaf = 0, minLoad = UINT32_MAX;
        for (uint32_t leaf = 0; leaf < g_nLeaves; ++leaf)
        {
            if (leafAssocCount[leaf] < minLoad)
            {
                minLoad = leafAssocCount[leaf];
                bestLeaf = leaf;
            }
        }
        topo.jobLeaves[j].push_back(bestLeaf);
        leafAssocCount[bestLeaf]++;
    }

    // 2b. 为每个 Job 选择 k 个 Worker Leaf（排除 PS 所在 Leaf，优先负载轻）
    for (uint32_t round = 0; round < topo.k; ++round)
    {
        for (uint32_t j = 0; j < g_numJobs; ++j)
        {
            uint32_t psLeaf = topo.jobLeaves[j][0];
            uint32_t bestLeaf = UINT32_MAX, minLoad = UINT32_MAX;

            for (uint32_t leaf = 0; leaf < g_nLeaves; ++leaf)
            {
                if (leaf == psLeaf)
                    continue;
                bool used = false;
                for (uint32_t x = 0; x < topo.jobLeaves[j].size(); ++x)
                    if (topo.jobLeaves[j][x] == leaf)
                    {
                        used = true;
                        break;
                    }
                if (used)
                    continue;
                if (leafAssocCount[leaf] < minLoad)
                {
                    minLoad = leafAssocCount[leaf];
                    bestLeaf = leaf;
                }
            }

            if (bestLeaf == UINT32_MAX)
            {
                // 回退：允许已用 Leaf，但仍排除 PS Leaf
                for (uint32_t leaf = 0; leaf < g_nLeaves; ++leaf)
                {
                    if (leaf == psLeaf)
                        continue;
                    if (leafAssocCount[leaf] < minLoad)
                    {
                        minLoad = leafAssocCount[leaf];
                        bestLeaf = leaf;
                    }
                }
            }

            NS_ABORT_MSG_IF(bestLeaf == UINT32_MAX,
                            "Job " + std::to_string(j) + " 无可用 Worker Leaf");
            topo.jobLeaves[j].push_back(bestLeaf);
            leafAssocCount[bestLeaf]++;
        }
    }

    // --- 3. 分配 Server（线性紧凑填充）---
    std::vector<uint32_t> leafNextLocal(g_nLeaves, 0);

    // 3a. 分配 PS（每个 PS 占 1 位）
    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        uint32_t psLeaf = topo.jobLeaves[j][0];
        uint32_t local = leafNextLocal[psLeaf];
        topo.psNodeIndices[j] = serverStartIdx + psLeaf * g_nServersPerLeaf + local;
        leafNextLocal[psLeaf]++;
    }

    // 3b. 分配 Worker（每 Leaf 占 chunkSize 个连续槽位）
    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        uint32_t wCount = 0;
        for (uint32_t i = 1; i <= topo.k; ++i)
        {
            uint32_t wLeaf = topo.jobLeaves[j][i];
            uint32_t localStart = leafNextLocal[wLeaf];

            NS_ABORT_MSG_IF(localStart + topo.chunkSize > g_nServersPerLeaf,
                            "Worker 越界: leaf=" + std::to_string(wLeaf) +
                                " start=" + std::to_string(localStart) +
                                " need=" + std::to_string(topo.chunkSize) +
                                " max=" + std::to_string(g_nServersPerLeaf));

            for (uint32_t offset = 0; offset < topo.chunkSize; ++offset)
            {
                uint32_t local = localStart + offset;
                topo.workerNodeIndices[j][wCount++] =
                    serverStartIdx + wLeaf * g_nServersPerLeaf + local;
            }
            leafNextLocal[wLeaf] += topo.chunkSize;
        }

        NS_ABORT_MSG_IF(wCount != g_workersPerJob, "Worker 数量不匹配: job=" + std::to_string(j));
    }

    // --- 4. Straggler 标记 ---
    if (g_stragglerDegree > 0)
    {
        std::uniform_int_distribution<uint32_t> dist(0, g_workersPerJob - 1);
        for (uint32_t j = 0; j < g_numJobs; ++j)
            topo.jobStragglerWorkerIdx[j] = dist(rng);
    }

    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        if (topo.jobStragglerWorkerIdx[j] != UINT32_MAX)
            topo.stragglerNodes.insert(topo.workerNodeIndices[j][topo.jobStragglerWorkerIdx[j]]);
    }

    // --- 5. 生成 Job 路由元数据（Spine-Leaf 配对）---
    // 策略：按 Job ID 轮询分配 Spine。
    // 恰好与 ECMP 计算的 Spine（dstLeaf % 8）完全对齐，ATP 和背景流必然共享同一上行队列。

    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        uint32_t jobId = j + 1;

        // 收集 Worker Leaf（排除 PS Leaf）
        std::set<uint32_t> workerLeaves;
        for (size_t x = 1; x < topo.jobLeaves[j].size(); ++x)
            workerLeaves.insert(topo.jobLeaves[j][x]);

        if (workerLeaves.empty())
            continue;

        // 轮询分配 Spine：Job 0->Spine0, Job 1->Spine1, ..., Job 7->Spine7
        uint32_t bestSpine = j % g_nSpines;

        for (uint32_t leaf : workerLeaves)
        {
            topo.jobRoutes[jobId].push_back({leaf, bestSpine});
        }
    }

    return topo;
}

// ============================================================
// 模块 1b：JSON 拓扑生成（读取外部 JSON 配置）
// ============================================================

/**
 * @brief 从 JSON 配置文件精确还原拓扑分配
 * @param serverStartIdx 服务器节点在全局 NodeContainer 中的起始偏移
 * @param jobsFile JSON jobs 配置文件路径
 * @param routesFile JSON routes 配置文件路径
 * @return TopoConfig 包含 PS/Worker 节点索引、Leaf 关联、路由元数据
 *
 * 要求：
 * - JSON 中 Worker 在 Leaf 上均匀分布（所有 Leaf 的 Worker 数相同 = chunkSize）；
 * - PS 与 Worker 不在同一 Leaf。
 */
static TopoConfig
GenerateTopologyFromJson(uint32_t serverStartIdx,
                         const std::string& jobsFile,
                         const std::string& routesFile)
{
    TopoConfig topo;
    topo.jobStragglerWorkerIdx.assign(g_numJobs, UINT32_MAX);
    topo.psNodeIndices.resize(g_numJobs);
    topo.workerNodeIndices.assign(g_numJobs, std::vector<uint32_t>(g_workersPerJob));
    topo.jobLeaves.resize(g_numJobs);

    // 解析 JSON
    std::vector<JobConfig> jobs = ParseJobsConfig(jobsFile);
    RouteConfig routeConfig = ParseRouteConfig(routesFile);

    NS_ABORT_MSG_IF(jobs.size() < g_numJobs,
                    "JSON jobs 数量不足: " + std::to_string(jobs.size()) + " < " +
                        std::to_string(g_numJobs));

    // 逐 Job 填充
    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        const auto& job = jobs[j];
        uint32_t jobId = job.id;
        NS_ABORT_MSG_IF(jobId != j + 1,
                        "Job ID 必须连续从 1 开始，期望 " + std::to_string(j + 1) + " 实际 " +
                            std::to_string(jobId));

        // PS
        topo.psNodeIndices[j] = ServerNameToIndex(job.ps, serverStartIdx);
        uint32_t psLeaf = LeafNameToIndex(job.ps_leaf);
        topo.jobLeaves[j].push_back(psLeaf);

        // Workers
        NS_ABORT_MSG_IF(job.workers.size() != g_workersPerJob,
                        "Job " + std::to_string(jobId) +
                            " Worker 数量不匹配 JSON: " + std::to_string(job.workers.size()) +
                            " != " + std::to_string(g_workersPerJob));

        for (uint32_t w = 0; w < g_workersPerJob; ++w)
        {
            topo.workerNodeIndices[j][w] = ServerNameToIndex(job.workers[w], serverStartIdx);
        }

        // 推导 chunkSize：统计每个 Worker Leaf 上的 Worker 数
        std::map<uint32_t, uint32_t> leafWorkerCount;
        for (uint32_t w = 0; w < g_workersPerJob; ++w)
        {
            uint32_t wNode = topo.workerNodeIndices[j][w];
            uint32_t wLeaf = getServerLeafIndex(wNode, serverStartIdx);
            leafWorkerCount[wLeaf]++;
        }

        uint32_t expectedChunk = leafWorkerCount.begin()->second;
        for (const auto& p : leafWorkerCount)
        {
            NS_ABORT_MSG_IF(p.second != expectedChunk,
                            "Job " + std::to_string(jobId) +
                                " Worker 在 Leaf 上分布不均匀，无法确定 chunkSize");
        }
        topo.chunkSize = expectedChunk;
        topo.k = leafWorkerCount.size(); // Worker Leaf 数量

        // 收集 Worker Leaves（去重，排除 PS Leaf）
        for (const auto& p : leafWorkerCount)
        {
            uint32_t wLeaf = p.first;
            if (wLeaf != psLeaf)
            {
                topo.jobLeaves[j].push_back(wLeaf);
            }
        }

        // 路由转换（leaf/spine 名称 -> 0-based 索引，排除 PS Leaf）
        if (routeConfig.jobRoutes.count(jobId))
        {
            for (const auto& route : routeConfig.jobRoutes.at(jobId))
            {
                uint32_t leafIdx = LeafNameToIndex(route.first);
                uint32_t spineIdx = SpineNameToIndex(route.second);
                if (leafIdx == psLeaf)
                    continue; // PS Leaf 本地直连，无需 ARO
                topo.jobRoutes[jobId].push_back({leafIdx, spineIdx});
            }
        }
    }

    // 计算 nAssocJobs（各 Leaf 关联 Job 数的最大值）
    std::vector<uint32_t> leafAssocCount(g_nLeaves, 0);
    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        for (uint32_t leaf : topo.jobLeaves[j])
        {
            leafAssocCount[leaf]++;
        }
    }
    topo.nAssocJobs = 0;
    for (uint32_t c : leafAssocCount)
    {
        if (c > topo.nAssocJobs)
            topo.nAssocJobs = c;
    }

    // Straggler（JSON 中未指定，保留随机逻辑）
    std::mt19937 rng(g_seed);
    if (g_stragglerDegree > 0)
    {
        std::uniform_int_distribution<uint32_t> dist(0, g_workersPerJob - 1);
        for (uint32_t j = 0; j < g_numJobs; ++j)
            topo.jobStragglerWorkerIdx[j] = dist(rng);
    }
    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        if (topo.jobStragglerWorkerIdx[j] != UINT32_MAX)
            topo.stragglerNodes.insert(topo.workerNodeIndices[j][topo.jobStragglerWorkerIdx[j]]);
    }

    return topo;
}

// ============================================================
// 模块 2：应用层安装
// ============================================================

/**
 * @brief 为所有 Job 安装 ATP 应用：PS 端 Sink + Worker 端 BulkSend
 * @param topo 拓扑分配结果
 * @param nodes 全局节点容器
 * @param serverNodes 服务器节点子集
 * @param leafServerInterfaces Leaf-Server 接口 IP（用于定位 PS IP）
 * @param dnn 模型参数
 * @param startTime 应用启动时间
 * @param stopTime 应用停止时间
 * @param runDirStr 本轮实验输出目录
 *
 * 关键配置：
 * - L4 单层聚合：Leaf 层聚合，Socket 处 fanInDegree0 配局部 Worker 数；
 * - PS 端 JobTree 期望收齐所有 Leaf 的聚合结果。
 */
static void
InstallApplications(const TopoConfig& topo,
                    NodeContainer& nodes,
                    NodeContainer& serverNodes,
                    std::vector<std::vector<Ipv4InterfaceContainer>>& leafServerInterfaces,
                    const DnnProfile& dnn,
                    Time startTime,
                    Time stopTime,
                    const std::string& runDirStr)
{
    g_sinkApps.resize(g_numJobs);
    g_runtimeJobs.clear();

    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        uint32_t jobId = j + 1;
        uint16_t sinkPort = 9 + j;
        uint16_t sendPort = 100 + j;

        // 定位 PS IP
        uint32_t psNodeIdx = topo.psNodeIndices[j];
        uint32_t psLeafIdx = getServerLeafIndex(psNodeIdx, g_nSpines + g_nLeaves);
        uint32_t psLocalIdx = getServerLocalIndex(psNodeIdx, g_nSpines + g_nLeaves);
        Ipv4Address psIp = leafServerInterfaces[psLeafIdx][psLocalIdx].GetAddress(1);

        // 初始化运行时状态
        DmlJobRuntime runtimeJob;
        runtimeJob.id = jobId;
        runtimeJob.modelName = dnn.name;
        runtimeJob.modelSizeBytes = dnn.modelSizeBytes;
        runtimeJob.computeTime = dnn.computeTime;
        runtimeJob.currentIteration = 0;
        runtimeJob.maxIterations = g_maxIterations;
        runtimeJob.startTime = startTime;
        runtimeJob.endTime = Seconds(0.0);
        g_runtimeJobs.push_back(runtimeJob);

        // --- PS 端：ATPPacketSink ---
        Ptr<ATPPacketSink> sinkApp = CreateObject<ATPPacketSink>();
        Address sinkAddr(InetSocketAddress(psIp, sinkPort));
        sinkApp->SetAddressPort(sinkAddr, sinkPort);

        Ptr<Socket> sinkSocket =
            Socket::CreateSocket(nodes.Get(psNodeIdx), ATPSocketFactory::GetTypeId());
        Ptr<ATPSocket> sinkATPSocket = DynamicCast<ATPSocket>(sinkSocket);
        sinkApp->SetSocket(sinkATPSocket);
        sinkATPSocket->Bind(sinkAddr);
        sinkATPSocket->Listen();

        sinkApp->SetStartTime(Seconds(0.0));
        sinkApp->SetStopTime(stopTime);
        nodes.Get(psNodeIdx)->AddApplication(sinkApp);

        g_sinkApps[j] = sinkApp;
        g_runtimeJobs[j].psSinkApp = sinkApp;

        // --- 按 Leaf 分组 Worker，构建聚合树 ---
        std::map<uint32_t, std::vector<uint32_t>> leafToWorkersMap;
        for (uint32_t w = 0; w < g_workersPerJob; ++w)
        {
            uint32_t wNode = topo.workerNodeIndices[j][w];
            uint32_t lIdx = getServerLeafIndex(wNode, g_nSpines + g_nLeaves);
            leafToWorkersMap[lIdx].push_back(wNode);
        }

        // 构造 PS 端 JobTree：fanInDegree1 = 全局 Leaf 聚合数
        JobTree tree(jobId);
        tree.fanInDegree1 = leafToWorkersMap.size();
        tree.fullBitmap1 =
            (leafToWorkersMap.size() >= 32) ? 0xFFFFFFFFu : ((1u << leafToWorkersMap.size()) - 1);

        int leafLogicalIdx = 0;
        uint32_t flatBitmap = 0;
        int globalWorkerCount = 0;
        std::map<uint32_t, int> leafLogicalIndexMap;

        for (auto& entry : leafToWorkersMap)
        {
            uint32_t count = static_cast<uint32_t>(entry.second.size()); // 该 Leaf 下 Worker 数
            tree.AddBranch(leafLogicalIdx, count, (1u << count) - 1u);
            for (uint32_t k = 0; k < count; ++k)
                flatBitmap |= (1u << (globalWorkerCount + k));
            globalWorkerCount += count;
            leafLogicalIndexMap[entry.first] = leafLogicalIdx;
            leafLogicalIdx++;
        }
        tree.expectedFlatBitmap = flatBitmap;

        sinkATPSocket->SetJobTree(tree);
        sinkATPSocket->AddAddressMapping(jobId, GenerateMulticastAddress(jobId), sendPort);

        // 创建 Job 吞吐量输出文件
        std::string jobPath = runDirStr + "/job" + std::to_string(jobId) + "-throughput.txt";
        if (j >= g_sinkBytesStreams.size())
            g_sinkBytesStreams.resize(j + 1);
        g_sinkBytesStreams[j].open(jobPath, std::ofstream::out | std::ofstream::trunc);

        // --- Worker 端：ATPBulkSendApplication ---
        for (uint32_t w = 0; w < g_workersPerJob; ++w)
        {
            uint32_t wNode = topo.workerNodeIndices[j][w];
            uint32_t wLeaf = getServerLeafIndex(wNode, g_nSpines + g_nLeaves);

            // 仅首个 Worker 开启拥塞窗口跟踪
            if (w == 0)
            {
                std::string cwndPath = runDirStr + "/job" + std::to_string(jobId) + "-cwnd.txt";
                if (j >= g_cwndStreams.size())
                    g_cwndStreams.resize(j + 1);
                g_cwndStreams[j].open(cwndPath, std::ofstream::out | std::ofstream::trunc);
                g_cwndStreams[j] << g_startTimeUs << "\t" << g_initCwnd << std::endl;
            }

            Ptr<Socket> ws = Socket::CreateSocket(nodes.Get(wNode), ATPSocketFactory::GetTypeId());
            Ptr<ATPSocket> atpSocket = DynamicCast<ATPSocket>(ws);
            atpSocket->SetInitCwnd(g_initCwnd);
            atpSocket->SetRetxTimeout(MicroSeconds(1000));
            atpSocket->SetRetxCheckInterval(MicroSeconds(200));

            Ptr<ATPBulkSendApplication> app = CreateObject<ATPBulkSendApplication>();
            app->Setup(sinkAddr, atpSocket, 0, jobId);
            app->SetEnableATPTag(true);
            app->SetStartTime(startTime);
            app->SetStopTime(stopTime);

            // 计算该 Worker 在局部 Leaf 中的索引，用于位图
            const auto& localWorkers = leafToWorkersMap[wLeaf];
            int myIdxInLeaf = -1;
            for (size_t i = 0; i < localWorkers.size(); ++i)
            {
                if (localWorkers[i] == wNode)
                {
                    myIdxInLeaf = static_cast<int>(i);
                    break;
                }
            }

            // L4 单层聚合配置：
            // fanInDegree0 = 局部 Leaf 内 Worker 数（局部聚合度）
            // fanInDegree1 = 全局 Leaf 数（全局聚合度）
            app->SetFaninDegree0(localWorkers.size());
            app->SetBitmap0(1u << myIdxInLeaf);
            app->SetFaninDegree1(leafToWorkersMap.size());
            app->SetBitmap1(1u << leafLogicalIndexMap[wLeaf]);

            atpSocket->SetConnectCallback(
                MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, app),
                MakeCallback(&ATPBulkSendApplication::ConnectionFailed, app));
            atpSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, app));

            Address workerAddr(InetSocketAddress(Ipv4Address::GetAny(), sendPort));
            atpSocket->Bind(workerAddr);
            atpSocket->Connect(sinkAddr);

            // 首个 Worker 绑定 cwnd 跟踪
            if (w == 0)
            {
                atpSocket->GetTxBuffer()->TraceConnectWithoutContext(
                    "cwndTrace",
                    MakeBoundCallback(&CwndChange, j));
            }

            nodes.Get(wNode)->AddApplication(app);
            g_runtimeJobs[j].workerApps.push_back(app);
        }
    }
}

// ============================================================
// 模块 3：路由配置（静态 + ARO + 组播）
// ============================================================

/**
 * @brief 配置三层网络路由：Server/Leaf/Spine 静态路由 + ARO 覆盖 + 组播树
 * @param topo 拓扑分配结果
 * @param serverStartIdx 服务器节点起始偏移
 *
 * L4 聚合开关：
 * - Spine 层关闭聚合（SetEnableAggregation(false)），仅做透明转发；
 * - Leaf 层开启聚合（SetEnableAggregation(true)），收齐同 Leaf 下所有 Worker 后向上游聚合。
 */
static void
ConfigureRouting(const TopoConfig& topo,
                 NodeContainer& spineNodes,
                 NodeContainer& leafNodes,
                 NodeContainer& serverNodes,
                 std::vector<std::vector<NetDeviceContainer>>& spineLeafDevices,
                 std::vector<std::vector<Ipv4InterfaceContainer>>& spineLeafInterfaces,
                 std::vector<std::vector<NetDeviceContainer>>& leafServerDevices,
                 std::vector<std::vector<Ipv4InterfaceContainer>>& leafServerInterfaces,
                 uint32_t serverStartIdx)
{
    NS_LOG_FUNCTION_NOARGS();

    ATPStaticRoutingHelper staticRoutingHelper;
    std::vector<Ptr<ATPStaticRouting>> spineRouting(g_nSpines);
    std::vector<Ptr<ATPStaticRouting>> leafRouting(g_nLeaves);
    uint32_t nTotalServers = g_nLeaves * g_nServersPerLeaf;
    std::vector<Ptr<ATPStaticRouting>> serverRouting(nTotalServers);

    // 初始化各层路由句柄
    for (uint32_t i = 0; i < g_nSpines; ++i)
    {
        spineRouting[i] =
            staticRoutingHelper.GetStaticRouting(spineNodes.Get(i)->GetObject<Ipv4>());
        spineRouting[i]->SetEnableAggregation(false);
    }
    for (uint32_t i = 0; i < g_nLeaves; ++i)
    {
        leafRouting[i] = staticRoutingHelper.GetStaticRouting(leafNodes.Get(i)->GetObject<Ipv4>());
        leafRouting[i]->SetEnableAggregation(true);
    }
    for (uint32_t i = 0; i < nTotalServers; ++i)
    {
        serverRouting[i] =
            staticRoutingHelper.GetStaticRouting(serverNodes.Get(i)->GetObject<Ipv4>());
    }
    NS_LOG_INFO("Routing handles initialized: " << g_nSpines << " spines, " << g_nLeaves
                                                << " leaves, " << nTotalServers << " servers.");

    // --- Server 路由：默认网关指向所属 Leaf ---
    NS_LOG_INFO("Configuring Server default gateway routes...");
    for (uint32_t s = 0; s < nTotalServers; ++s)
    {
        uint32_t leafIdx = getServerLeafIndex(serverStartIdx + s, serverStartIdx);
        uint32_t localIdx = getServerLocalIndex(serverStartIdx + s, serverStartIdx);
        Ipv4Address gw = leafServerInterfaces[leafIdx][localIdx].GetAddress(0);
        for (uint32_t t = 0; t < nTotalServers; ++t)
        {
            if (t == s)
                continue;
            uint32_t tLeaf = getServerLeafIndex(serverStartIdx + t, serverStartIdx);
            uint32_t tLocal = getServerLocalIndex(serverStartIdx + t, serverStartIdx);
            Ipv4Address dst = leafServerInterfaces[tLeaf][tLocal].GetAddress(1);
            serverRouting[s]->AddHostRouteTo(dst, gw, 1);
        }
    }
    NS_LOG_INFO("Server routes configured.");

    // --- ARO 覆盖：Worker Leaf 到 PS 的下一跳指向指定 Spine ---
    NS_LOG_INFO("Applying ARO host-routes for ATP traffic...");
    uint32_t aroRouteCount = 0;
    for (const auto& jobEntry : topo.jobRoutes)
    {
        uint32_t jobId = jobEntry.first;
        uint32_t j = jobId - 1;
        uint32_t psNodeIdx = topo.psNodeIndices[j];
        uint32_t psLeafIdx = getServerLeafIndex(psNodeIdx, serverStartIdx);
        uint32_t psLocalIdx = getServerLocalIndex(psNodeIdx, serverStartIdx);
        Ipv4Address psIp = leafServerInterfaces[psLeafIdx][psLocalIdx].GetAddress(1);

        NS_LOG_DEBUG("Job " << jobId << " PS at Leaf" << psLeafIdx << " (IP=" << psIp
                            << "), ARO routes from Worker Leaves:");

        for (const auto& route : jobEntry.second)
        {
            uint32_t leafIdx = route.first;
            uint32_t spineIdx = route.second;
            if (leafIdx == psLeafIdx)
                continue;

            Ipv4Address nextHop = spineLeafInterfaces[spineIdx][leafIdx].GetAddress(0);
            leafRouting[leafIdx]->AddHostRouteTo(psIp, nextHop, spineIdx + 1);
            aroRouteCount++;

            NS_LOG_DEBUG("  [ARO] Leaf" << leafIdx << " -> PS_IP=" << psIp << " via Spine"
                                        << spineIdx << " (nextHop=" << nextHop
                                        << ", outIface=" << (spineIdx + 1) << ")");
        }
    }
    NS_LOG_INFO("ARO host-routes applied: " << aroRouteCount << " entries.");

    // --- Leaf 路由：Server 子网直连，跨 Leaf 流量经 Spine 选择 ---
    NS_LOG_INFO("Configuring Leaf default routes (local + ARO/ECMP)...");
    for (uint32_t l = 0; l < g_nLeaves; ++l)
    {
        // A. 本地直连 Server
        for (uint32_t k = 0; k < g_nServersPerLeaf; ++k)
        {
            Ipv4Address srv = leafServerInterfaces[l][k].GetAddress(1);
            leafRouting[l]->AddHostRouteTo(srv, srv, g_nSpines + k + 1);
        }

        // B. 远端 Server：使用 ECMP
        for (uint32_t tl = 0; tl < g_nLeaves; ++tl)
        {
            if (tl == l)
                continue;

            uint32_t spineChoice = tl % g_nSpines;
            Ipv4Address spineGw = spineLeafInterfaces[spineChoice][l].GetAddress(0);

            for (uint32_t k = 0; k < g_nServersPerLeaf; ++k)
            {
                Ipv4Address dst = leafServerInterfaces[tl][k].GetAddress(1);
                leafRouting[l]->AddHostRouteTo(dst, spineGw, spineChoice + 1);
            }
        }
    }

    // --- Spine 路由：向下分发到各 Leaf Server 子网 ---
    NS_LOG_INFO("Configuring Spine downlink routes...");
    for (uint32_t s = 0; s < g_nSpines; ++s)
    {
        for (uint32_t l = 0; l < g_nLeaves; ++l)
        {
            Ipv4Address gw = spineLeafInterfaces[s][l].GetAddress(1);
            for (uint32_t k = 0; k < g_nServersPerLeaf; ++k)
            {
                Ipv4Address dst = leafServerInterfaces[l][k].GetAddress(1);
                spineRouting[s]->AddHostRouteTo(dst, gw, l + 1);
            }
        }
    }
    NS_LOG_INFO("Spine routes configured.");

    // --- 组播路由：构建 PS 到 所有 Worker Leaf 的组播分发树 ---
    NS_LOG_INFO("Configuring multicast trees for " << g_numJobs << " jobs...");
    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        uint32_t jobId = j + 1;
        Ipv4Address multicastGroup = GenerateMulticastAddress(jobId);
        uint32_t psNodeIdx = topo.psNodeIndices[j];
        uint32_t psLeafIdx = getServerLeafIndex(psNodeIdx, serverStartIdx);
        uint32_t psLocalIdx = getServerLocalIndex(psNodeIdx, serverStartIdx);
        Ipv4Address psAddress = leafServerInterfaces[psLeafIdx][psLocalIdx].GetAddress(1);

        auto itRoutes = topo.jobRoutes.find(jobId);
        if (itRoutes == topo.jobRoutes.end())
        {
            NS_LOG_WARN("Job " << jobId << " has no ARO routes, skipping multicast.");
            continue;
        }

        NS_LOG_DEBUG("Job " << jobId << " multicast: group=" << multicastGroup
                            << " PS=" << psAddress << " at Leaf" << psLeafIdx);

        // 收集该 Job 涉及的 Spine 到 Leaf 映射
        std::map<uint32_t, std::vector<uint32_t>> spineToLeafsMap;
        std::set<uint32_t> activeSpines;
        for (const auto& route : itRoutes->second)
        {
            spineToLeafsMap[route.second].push_back(route.first);
            activeSpines.insert(route.second);
        }

        // 收集各 Worker Leaf 对应的出接口（指向 Worker Server）
        std::map<uint32_t, std::vector<uint32_t>> leafToWorkerInterfaces;
        for (uint32_t w = 0; w < g_workersPerJob; ++w)
        {
            uint32_t wNode = topo.workerNodeIndices[j][w];
            uint32_t wLeafIdx = getServerLeafIndex(wNode, serverStartIdx);
            uint32_t wLocalIdx = getServerLocalIndex(wNode, serverStartIdx);
            leafToWorkerInterfaces[wLeafIdx].push_back(g_nSpines + wLocalIdx + 1);
        }

        // PS 所在 Server 设置默认组播路由
        serverRouting[psNodeIdx - serverStartIdx]->SetDefaultMulticastRoute(1);

        // PS 所在 Leaf：入接口来自 PS，出接口指向各 Spine 及本地 Worker
        {
            uint32_t inputFromPS = g_nSpines + psLocalIdx + 1;
            std::vector<uint32_t> out;
            for (uint32_t sIdx : activeSpines)
                out.push_back(sIdx + 1);
            auto it = leafToWorkerInterfaces.find(psLeafIdx);
            if (it != leafToWorkerInterfaces.end())
            {
                for (uint32_t ifIdx : it->second)
                    out.push_back(ifIdx);
            }
            leafRouting[psLeafIdx]->AddMulticastRoute(psAddress, multicastGroup, inputFromPS, out);
            NS_LOG_DEBUG("  PS-Leaf" << psLeafIdx << " mcast out-ifaces=" << out.size());
        }

        // Spine 交换机：入接口来自 PS Leaf，出接口指向下游 Leaf
        for (uint32_t sIdx : activeSpines)
        {
            std::vector<uint32_t> out;
            for (uint32_t tl : spineToLeafsMap[sIdx])
            {
                if (tl != psLeafIdx)
                    out.push_back(tl + 1);
            }
            if (!out.empty())
            {
                spineRouting[sIdx]->AddMulticastRoute(psAddress,
                                                      multicastGroup,
                                                      psLeafIdx + 1,
                                                      out);
                NS_LOG_DEBUG("  Spine" << sIdx << " mcast out-ifaces=" << out.size());
            }
        }

        // 目标 Leaf：入接口来自 Spine，出接口指向本地 Worker Server
        for (const auto& entry : spineToLeafsMap)
        {
            uint32_t sIdx = entry.first;
            for (uint32_t leafIdx : entry.second)
            {
                if (leafIdx == psLeafIdx)
                    continue;
                auto it = leafToWorkerInterfaces.find(leafIdx);
                if (it == leafToWorkerInterfaces.end())
                    continue;
                leafRouting[leafIdx]->AddMulticastRoute(psAddress,
                                                        multicastGroup,
                                                        sIdx + 1,
                                                        it->second);
                NS_LOG_DEBUG("  Worker-Leaf" << leafIdx << " mcast in-iface=" << (sIdx + 1)
                                             << " out-ifaces=" << it->second.size());
            }
        }
    }
    NS_LOG_INFO("Multicast tree configuration complete.");
}

// ============================================================
// 模块 4：背景流量
// ============================================================

/**
 * @brief 在所有服务器间注入 UDP 背景流（老鼠流/大象流混合）
 * @param serverNodes 服务器节点容器
 * @param leafServerInterfaces Leaf-Server 接口（用于获取目的 IP）
 * @param stopTime 背景流停止时间
 * @param serverStartIdx 服务器节点起始偏移
 *
 * 背景流使用 OnOff 应用模拟泊松到达的 UDP 流，与 ATP 流量共享链路。
 */
static void
InstallBackgroundTraffic(NodeContainer& serverNodes,
                         std::vector<std::vector<Ipv4InterfaceContainer>>& leafServerInterfaces,
                         Time stopTime,
                         uint32_t serverStartIdx)
{
    uint32_t nTotalServers = g_nLeaves * g_nServersPerLeaf;
    uint16_t bgPortBase = 5000;
    ApplicationContainer bgSinks;

    // 在所有服务器上安装 UDP PacketSink
    for (uint32_t i = 0; i < nTotalServers; i++)
    {
        PacketSinkHelper bgSinkHelper("ns3::UdpSocketFactory",
                                      InetSocketAddress(Ipv4Address::GetAny(), bgPortBase));
        ApplicationContainer sinkApp = bgSinkHelper.Install(serverNodes.Get(i));
        bgSinks.Add(sinkApp);
    }
    bgSinks.Start(Seconds(0.0));
    bgSinks.Stop(stopTime);

    // 配置 OnOff 源：恒定速率、指数间隔到达
    OnOffHelper bgSourceHelper("ns3::UdpSocketFactory", Address());
    bgSourceHelper.SetAttribute("DataRate", StringValue(g_bgDataRate));
    bgSourceHelper.SetAttribute("PacketSize", UintegerValue(g_bgPacketSize));
    bgSourceHelper.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=1.0]"));
    bgSourceHelper.SetAttribute("OffTime",
                                StringValue("ns3::ConstantRandomVariable[Constant=0.0]"));

    ApplicationContainer bgSources;

    Ptr<ExponentialRandomVariable> expRand = CreateObject<ExponentialRandomVariable>();
    expRand->SetAttribute("Mean", DoubleValue(g_bgMeanInterval));

    Ptr<UniformRandomVariable> hostRand = CreateObject<UniformRandomVariable>();
    Ptr<UniformRandomVariable> sizeProbRand = CreateObject<UniformRandomVariable>();

    double currentStartTime = g_bgStartTime;
    uint32_t globalFlowCount = 0;

    // 泊松生成背景流，直到时间或数量上限
    while (currentStartTime < stopTime.GetSeconds() && globalFlowCount < g_bgMaxFlows)
    {
        uint32_t srcId = hostRand->GetInteger(0, nTotalServers - 1);
        uint32_t dstId = hostRand->GetInteger(1, nTotalServers - 1);
        dstId = (dstId + srcId) % nTotalServers; // 避免自环

        uint32_t leafIdx = getServerLeafIndex(serverStartIdx + dstId, serverStartIdx);
        uint32_t localIdx = getServerLocalIndex(serverStartIdx + dstId, serverStartIdx);
        Ipv4Address dstIp = leafServerInterfaces[leafIdx][localIdx].GetAddress(1);

        // 按概率选择老鼠流或大象流
        double prob = sizeProbRand->GetValue();
        uint32_t flowBytes = (prob < g_bgMouseProb) ? g_bgMouseBytes : g_bgElephantBytes;

        bgSourceHelper.SetAttribute("MaxBytes", UintegerValue(flowBytes));
        bgSourceHelper.SetAttribute("Remote", AddressValue(InetSocketAddress(dstIp, bgPortBase)));

        ApplicationContainer flowApp = bgSourceHelper.Install(serverNodes.Get(srcId));
        flowApp.Start(Seconds(currentStartTime));
        flowApp.Stop(stopTime);
        bgSources.Add(flowApp);

        currentStartTime += expRand->GetValue();
        globalFlowCount++;
    }
}

// ============================================================
// 主函数
// ============================================================

int
main(int argc, char* argv[])
{
    // LogComponentEnable("Main-Experiment", LOG_LEVEL_DEBUG);

    // --- 命令行解析 ---
    CommandLine cmd(__FILE__);
    cmd.AddValue("numJobs", "Job 数量", g_numJobs);
    cmd.AddValue("workersPerJob", "每 Job Worker 数", g_workersPerJob);
    cmd.AddValue("switchMemoryKB", "交换机缓存 KB", g_switchMemoryKB);
    cmd.AddValue("stragglerDegree", "0=无、1=轻度、2=严重", g_stragglerDegree);
    cmd.AddValue("dnnModel", "resnet50/vgg19/gpt2/bert", g_dnnModel);
    cmd.AddValue("durationUs", "仿真时长 us", g_durationUs);
    cmd.AddValue("linkDelayUs", "链路延迟 us", g_linkDelayUs);
    cmd.AddValue("outputDir", "输出目录", g_outputDir);
    cmd.AddValue("initCwnd", "初始拥塞窗口", g_initCwnd);
    cmd.AddValue("enableEcn", "启用 ECN", g_enableEcn);
    cmd.AddValue("spineEcnThresholdPkts", "Spine 端 ECN 阈值 包", g_spineEcnThresholdPkts);
    cmd.AddValue("leafEcnThresholdPkts", "Leaf 端 ECN 阈值 包", g_leafEcnThresholdPkts);
    cmd.AddValue("maxIterations", "每 Job 训练迭代数", g_maxIterations);
    cmd.AddValue("nSpines", "Spine 交换机数", g_nSpines);
    cmd.AddValue("nLeaves", "Leaf 交换机数", g_nLeaves);
    cmd.AddValue("nServersPerLeaf", "每 Leaf 服务器数", g_nServersPerLeaf);
    cmd.AddValue("seed", "随机种子", g_seed);

    cmd.AddValue("topoMode", "拓扑生成模式: auto | json", g_topoMode);
    cmd.AddValue("jobsConfig", "JSON jobs 配置文件路径 (json 模式)", g_jobsConfigFile);
    cmd.AddValue("routeConfig", "JSON routes 配置文件路径 (json 模式)", g_routeConfigFile);

    cmd.AddValue("bgEnable", "启用背景流", g_bgEnable);
    cmd.AddValue("bgDataRate", "背景流速率", g_bgDataRate);
    cmd.AddValue("bgPacketSize", "背景包大小", g_bgPacketSize);
    cmd.AddValue("bgMeanInterval", "背景流泊松间隔均值 秒", g_bgMeanInterval);
    cmd.AddValue("bgMaxFlows", "背景流最大数量", g_bgMaxFlows);
    cmd.AddValue("bgStartTime", "背景流启动时间 秒", g_bgStartTime);
    cmd.AddValue("bgMouseProb", "老鼠流概率", g_bgMouseProb);
    cmd.AddValue("bgMouseBytes", "老鼠流字节数", g_bgMouseBytes);
    cmd.AddValue("bgElephantBytes", "大象流字节数", g_bgElephantBytes);
    cmd.Parse(argc, argv);

    DnnProfile dnn = GetDnnProfile(g_dnnModel);

    // 前置约束检查
    NS_ABORT_MSG_IF(g_workersPerJob == 0 || g_workersPerJob > 32,
                    "ATP 使用 uint32_t 位图，workersPerJob 需在 1~32");
    NS_ABORT_MSG_IF(g_nSpines == 0 || g_nLeaves == 0 || g_nServersPerLeaf == 0, "拓扑维度必须为正");

    uint32_t nTotalServers = g_nLeaves * g_nServersPerLeaf;
    uint32_t serverStartIdx = g_nSpines + g_nLeaves;
    uint32_t nTotalNodes = g_nSpines + g_nLeaves + nTotalServers;
    uint32_t totalWorkers = g_numJobs * g_workersPerJob;
    uint32_t neededServers = g_numJobs + totalWorkers;

    NS_ABORT_MSG_IF(neededServers > nTotalServers, "服务器数量不足以部署所有 PS 和 Worker");

    // 阶段 1：生成拓扑
    TopoConfig topo;
    if (g_topoMode == "json")
    {
        NS_ABORT_MSG_IF(g_jobsConfigFile.empty(), "json 模式必须指定 --jobsConfig");
        NS_ABORT_MSG_IF(g_routeConfigFile.empty(), "json 模式必须指定 --routeConfig");
        NS_LOG_INFO("Using JSON topology mode.");
        topo = GenerateTopologyFromJson(serverStartIdx, g_jobsConfigFile, g_routeConfigFile);
    }
    else
    {
        NS_LOG_INFO("Using auto topology mode.");
        topo = GenerateTopologyAuto(serverStartIdx);
    }

    // 阶段 2：创建节点与链路
    NodeContainer nodes;
    nodes.Create(nTotalNodes);

    NodeContainer spineNodes, leafNodes, serverNodes;
    for (uint32_t i = 0; i < g_nSpines; ++i)
        spineNodes.Add(nodes.Get(i));
    for (uint32_t i = 0; i < g_nLeaves; ++i)
        leafNodes.Add(nodes.Get(g_nSpines + i));
    for (uint32_t i = 0; i < nTotalServers; ++i)
        serverNodes.Add(nodes.Get(serverStartIdx + i));

    // 配置链路属性：Spine-Leaf 100Gbps，Leaf-Server 100Gbps（straggler 降级）
    std::ostringstream qSizeStr;
    qSizeStr << g_switchMemoryKB << "KB";

    PointToPointHelper p2pSpineLeaf;
    p2pSpineLeaf.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    p2pSpineLeaf.SetChannelAttribute("Delay", StringValue(std::to_string(g_linkDelayUs) + "us"));
    p2pSpineLeaf.SetQueue("ns3::DropTailQueue", "MaxSize", StringValue(qSizeStr.str()));

    PointToPointHelper p2pLeafServer;
    p2pLeafServer.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    p2pLeafServer.SetChannelAttribute("Delay", StringValue(std::to_string(g_linkDelayUs) + "us"));
    p2pLeafServer.SetQueue("ns3::DropTailQueue", "MaxSize", StringValue(qSizeStr.str()));

    // 存储设备与接口容器，供后续路由配置使用
    std::vector<std::vector<NetDeviceContainer>> spineLeafDevices(
        g_nSpines,
        std::vector<NetDeviceContainer>(g_nLeaves));
    std::vector<std::vector<Ipv4InterfaceContainer>> spineLeafInterfaces(
        g_nSpines,
        std::vector<Ipv4InterfaceContainer>(g_nLeaves));
    std::vector<std::vector<NetDeviceContainer>> leafServerDevices(
        g_nLeaves,
        std::vector<NetDeviceContainer>(g_nServersPerLeaf));
    std::vector<std::vector<Ipv4InterfaceContainer>> leafServerInterfaces(
        g_nLeaves,
        std::vector<Ipv4InterfaceContainer>(g_nServersPerLeaf));

    // 安装 Spine-Leaf 链路并启用 ECN
    for (uint32_t s = 0; s < g_nSpines; ++s)
    {
        for (uint32_t l = 0; l < g_nLeaves; ++l)
        {
            NodeContainer pair(spineNodes.Get(s), leafNodes.Get(l));
            NetDeviceContainer dev = p2pSpineLeaf.Install(pair);
            spineLeafDevices[s][l] = dev;

            Ptr<PointToPointNetDevice> spineDev = DynamicCast<PointToPointNetDevice>(dev.Get(0));
            if (g_bgEnable)
            {
                spineDev->SetThresholdBytes(g_spineEcnThresholdPkts * g_packetSize);
                spineDev->SetEnableEcnBytes(g_enableEcn);
            }
            else
            {
                spineDev->SetThreshold(g_spineEcnThresholdPkts);
                spineDev->SetEnableEcn(g_enableEcn);
            }

            Ptr<PointToPointNetDevice> leafDev = DynamicCast<PointToPointNetDevice>(dev.Get(1));

            if (g_bgEnable)
            {
                leafDev->SetThresholdBytes(g_leafEcnThresholdPkts * g_packetSize);
                leafDev->SetEnableEcnBytes(g_enableEcn);
            }
            else
            {
                leafDev->SetThreshold(g_leafEcnThresholdPkts);
                leafDev->SetEnableEcn(g_enableEcn);
            }
        }
    }

    // 安装 Leaf-Server 链路；straggler 节点链路降速
    for (uint32_t l = 0; l < g_nLeaves; ++l)
    {
        for (uint32_t k = 0; k < g_nServersPerLeaf; ++k)
        {
            uint32_t serverNodeIdx = serverStartIdx + l * g_nServersPerLeaf + k;
            NodeContainer pair(leafNodes.Get(l), nodes.Get(serverNodeIdx));

            if (topo.stragglerNodes.count(serverNodeIdx))
            {
                double frac = (g_stragglerDegree == 1) ? 0.5 : 0.25;
                p2pLeafServer.SetDeviceAttribute(
                    "DataRate",
                    StringValue(std::to_string(100.0 * frac) + "Gbps"));
            }
            else
            {
                p2pLeafServer.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
            }

            NetDeviceContainer dev = p2pLeafServer.Install(pair);
            leafServerDevices[l][k] = dev;
            p2pLeafServer.SetDeviceAttribute("DataRate", StringValue("100Gbps")); // 恢复默认值

            Ptr<PointToPointNetDevice> leafDev = DynamicCast<PointToPointNetDevice>(dev.Get(0));

            if (g_bgEnable)
            {
                leafDev->SetThresholdBytes(g_leafEcnThresholdPkts * g_packetSize);
                leafDev->SetEnableEcnBytes(g_enableEcn);
            }
            else
            {
                leafDev->SetThreshold(g_leafEcnThresholdPkts);
                leafDev->SetEnableEcn(g_enableEcn);
            }
        }
    }

    // 安装互联网协议栈（使用 ATP 静态路由）
    InternetStackHelper internet;
    internet.SetRoutingHelper(ATPStaticRoutingHelper());
    internet.Install(nodes);

    // 分配 IP 地址
    Ipv4AddressHelper ipv4;
    for (uint32_t s = 0; s < g_nSpines; ++s)
    {
        for (uint32_t l = 0; l < g_nLeaves; ++l)
        {
            std::ostringstream subnet;
            subnet << "10." << (s + 1) << "." << (l + 1) << ".0";
            ipv4.SetBase(subnet.str().c_str(), "255.255.255.0");
            spineLeafInterfaces[s][l] = ipv4.Assign(spineLeafDevices[s][l]);
        }
    }

    for (uint32_t l = 0; l < g_nLeaves; ++l)
    {
        for (uint32_t k = 0; k < g_nServersPerLeaf; ++k)
        {
            std::ostringstream subnet;
            subnet << "192." << (l + 100) << "." << (k + 1) * 10 << ".0";
            ipv4.SetBase(subnet.str().c_str(), "255.255.255.0");
            leafServerInterfaces[l][k] = ipv4.Assign(leafServerDevices[l][k]);
        }
    }

    // 阶段 3：配置路由（静态 + ARO + 组播）
    ConfigureRouting(topo,
                     spineNodes,
                     leafNodes,
                     serverNodes,
                     spineLeafDevices,
                     spineLeafInterfaces,
                     leafServerDevices,
                     leafServerInterfaces,
                     serverStartIdx);

    // 阶段 4：安装 ATP 应用
    std::ostringstream runDir;
    runDir << g_outputDir << "/"
           << "J" << g_numJobs << "_W" << g_workersPerJob << "_M" << g_switchMemoryKB << "_S"
           << g_stragglerDegree << "_" << g_dnnModel << "_Sp" << g_nSpines << "_Le" << g_nLeaves
           << "_Se" << g_nServersPerLeaf << "_I" << g_maxIterations << "_Sd" << g_seed << "_T"
           << g_topoMode;
    if (g_bgEnable)
        runDir << "_BG";
    std::string runDirStr = runDir.str();

    EnsureDir(g_outputDir);
    EnsureDir(runDirStr);

    // 写入实验参数汇总
    std::string summaryPath = runDirStr + "/summary.txt";
    g_summaryStream.open(summaryPath, std::ofstream::out | std::ofstream::trunc);
    g_summaryStream << "实验参数:\n"
                    << "  numJobs=" << g_numJobs << "\n"
                    << "  workersPerJob=" << g_workersPerJob << "\n"
                    << "  switchMemoryKB=" << g_switchMemoryKB << "\n"
                    << "  stragglerDegree=" << g_stragglerDegree << "\n"
                    << "  dnnModel=" << g_dnnModel << "\n"
                    << "  maxIterations=" << g_maxIterations << "\n"
                    << "  modelSizeBytes=" << dnn.modelSizeBytes << "\n"
                    << "  durationUs=" << g_durationUs << "\n"
                    << "  linkDelayUs=" << g_linkDelayUs << "\n"
                    << "  nSpines=" << g_nSpines << "\n"
                    << "  nLeaves=" << g_nLeaves << "\n"
                    << "  nServersPerLeaf=" << g_nServersPerLeaf << "\n"
                    << "  bgEnable=" << g_bgEnable << "\n"
                    << "----------------------------------------\n";

    g_startTimeUs = 1000000;
    Time startTime = Seconds(1.0);
    Time stopTime = startTime + MicroSeconds(g_durationUs);
    g_simStartTime = startTime;
    g_lastJobBytes.resize(g_numJobs, 0);

    InstallApplications(topo,
                        nodes,
                        serverNodes,
                        leafServerInterfaces,
                        dnn,
                        startTime,
                        stopTime,
                        runDirStr);

    // 阶段 5：背景流量
    if (g_bgEnable)
    {
        InstallBackgroundTraffic(serverNodes, leafServerInterfaces, stopTime, serverStartIdx);
    }

    // 阶段 6：调度测量与迭代控制
    std::string qPath = runDirStr + "/queue-size.txt";
    g_queueStream.open(qPath, std::ofstream::out | std::ofstream::trunc);

    uint32_t ps0Leaf = getServerLeafIndex(topo.psNodeIndices[0], serverStartIdx);
    Simulator::Schedule(startTime,
                        &SampleQueue,
                        DynamicCast<PointToPointNetDevice>(spineLeafDevices[0][ps0Leaf].Get(0)));
    Simulator::Schedule(startTime, &Measurement, g_numJobs);

    for (const auto& job : g_runtimeJobs)
    {
        Simulator::Schedule(startTime + MilliSeconds(1),
                            &DmlJobCoordinator::CheckIterationProgress,
                            job.id);
    }

    // 运行仿真
    Simulator::Stop(stopTime);
    Simulator::Run();
    Simulator::Destroy();

    // 阶段 7：汇总输出
    double durSec = (Simulator::Now() - g_simStartTime).GetSeconds();
    if (durSec <= 0.0)
        durSec = g_durationUs / 1e6;

    double actualDurSec = 0.0;

    for (uint32_t j = 0; j < g_numJobs; ++j)
    {
        uint64_t totalRx = g_sinkApps[j]->GetTotalRxJob(j + 1);
        double jobDurSec = durSec;

        if (g_runtimeJobs[j].endTime > g_runtimeJobs[j].startTime)
        {
            jobDurSec = (g_runtimeJobs[j].endTime - g_runtimeJobs[j].startTime).GetSeconds();
        }

        if (jobDurSec > actualDurSec)
        {
            actualDurSec = jobDurSec;
        }

        double tpMbps = (totalRx * 8.0) / (jobDurSec * 1e6);
        g_summaryStream << "Job" << (j + 1) << "_TotalRxBytes=" << totalRx << "\n";
        g_summaryStream << "Job" << (j + 1) << "_ThroughputMbps=" << tpMbps << "\n";
        std::cout << "Job" << (j + 1) << ": " << totalRx << " bytes, " << tpMbps << " Mbps\n";
    }
    g_summaryStream << "ActualDurationSec=" << actualDurSec << "\n";
    g_summaryStream << "TotalRxBytes=" << [&]() {
        uint64_t sum = 0;
        for (auto& s : g_sinkApps)
            sum += s->GetTotalRx();
        return sum;
    }() << "\n";

    // 关闭输出流
    for (auto& s : g_cwndStreams)
        s.close();
    for (auto& s : g_sinkBytesStreams)
        s.close();
    g_queueStream.close();
    g_summaryStream.close();

    std::cout << "Results saved to: " << runDirStr << std::endl;
    return 0;
}