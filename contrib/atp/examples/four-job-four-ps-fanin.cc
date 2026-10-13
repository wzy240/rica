#include "../model/atp-l4-protocol.h"

#include "ns3/applications-module.h"
#include "ns3/atp-module.h"
#include "ns3/core-module.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include "ns3/point-to-point-module.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

using namespace ns3;

namespace {

constexpr uint32_t kJobs = 4;
constexpr uint32_t kProvisionedWorkersPerJob = 24;
constexpr uint32_t kDefaultAggregatorSlots = 2048;
constexpr uint32_t kWarmupUs = 5000;
constexpr uint32_t kSampleIntervalUs = 10;

struct MeasurementState
{
    Ptr<ATPPacketSink> sinks[kJobs];
    Ptr<ATPL4Protocol> protocol;
    Ptr<PointToPointNetDevice> psEgressDevices[kJobs];
    uint64_t warmupBytes[kJobs]{};
    uint64_t occupancySamples{0};
    uint64_t occupancySum[kJobs]{};
    uint32_t occupancyMax[kJobs]{};
    uint64_t queueSamples{0};
    uint64_t queuePacketSum[kJobs]{};
    uint32_t queuePacketMax[kJobs]{};
    uint64_t totalQueuePacketSum{0};
    uint32_t totalQueuePacketMax{0};
    Time measurementEnd;
    Time sampleInterval;
};

MeasurementState g_measurement;

template <typename T>
auto
SetWorkerCount(const Ptr<T>& socket, uint32_t workers, int)
    -> decltype(socket->SetNumWorkers(workers), bool())
{
    socket->SetNumWorkers(workers);
    return true;
}

template <typename T>
bool
SetWorkerCount(const Ptr<T>&, uint32_t, long)
{
    return false;
}

uint32_t
InterfaceForDevice(const Ptr<Node>& node, const Ptr<NetDevice>& device)
{
    int32_t interface = node->GetObject<Ipv4>()->GetInterfaceForDevice(device);
    NS_ABORT_MSG_IF(interface < 0, "No IPv4 interface for device");
    return static_cast<uint32_t>(interface);
}

void
CaptureWarmupBytes()
{
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        g_measurement.warmupBytes[job] = g_measurement.sinks[job]->GetTotalRxJob(job + 1);
    }
}

void
SampleResources()
{
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        uint32_t active = g_measurement.protocol->GetAggregatorCount(job + 1);
        g_measurement.occupancySum[job] += active;
        g_measurement.occupancyMax[job] =
            std::max(g_measurement.occupancyMax[job], active);
    }
    ++g_measurement.occupancySamples;

    uint32_t totalQueued = 0;
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        uint32_t queued = g_measurement.psEgressDevices[job]->GetQueue()->GetNPackets();
        g_measurement.queuePacketSum[job] += queued;
        g_measurement.queuePacketMax[job] =
            std::max(g_measurement.queuePacketMax[job], queued);
        totalQueued += queued;
    }
    g_measurement.totalQueuePacketSum += totalQueued;
    g_measurement.totalQueuePacketMax =
        std::max(g_measurement.totalQueuePacketMax, totalQueued);
    ++g_measurement.queueSamples;

    Time next = Simulator::Now() + g_measurement.sampleInterval;
    if (next <= g_measurement.measurementEnd)
    {
        Simulator::Schedule(g_measurement.sampleInterval, &SampleResources);
    }
}

} // namespace

int
main(int argc, char* argv[])
{
    uint32_t fanin = 24;
    uint32_t aggregatorSlots = kDefaultAggregatorSlots;
    constexpr uint32_t initCwnd = 32;
    uint32_t durationUs = 10000;
    uint32_t ecnThresholdBytes = 32768;
    std::string algorithm = "unknown";
    std::string outputDir = "four-job-four-ps-fanin-results";

    CommandLine cmd(__FILE__);
    cmd.AddValue("fanin", "Active workers per job", fanin);
    cmd.AddValue("aggregatorSlots", "Shared switch aggregator slot count", aggregatorSlots);
    cmd.AddValue("durationUs", "Communication duration in microseconds", durationUs);
    cmd.AddValue("ecnThresholdBytes", "ECN marking threshold in bytes", ecnThresholdBytes);
    cmd.AddValue("algorithm", "Label written to the result", algorithm);
    cmd.AddValue("outputDir", "Result directory", outputDir);
    cmd.Parse(argc, argv);

    NS_ABORT_MSG_IF(fanin == 0 || fanin > kProvisionedWorkersPerJob,
                    "fanin must be in 1..24");
    NS_ABORT_MSG_IF(aggregatorSlots == 0, "aggregatorSlots must be positive");
    NS_ABORT_MSG_IF(kWarmupUs >= durationUs, "durationUs must be greater than 5000");
    NS_ABORT_MSG_IF(ecnThresholdBytes == 0 || ecnThresholdBytes > 262144,
                    "ecnThresholdBytes must be in 1..262144");

    std::filesystem::create_directories(outputDir);

    const uint32_t totalWorkers = kJobs * kProvisionedWorkersPerJob;
    const uint32_t switchIndex = totalWorkers;
    const uint32_t psBaseIndex = totalWorkers + 1;

    NodeContainer nodes;
    nodes.Create(totalWorkers + 1 + kJobs);
    Ptr<Node> aggregateSwitch = nodes.Get(switchIndex);
    Ptr<Node> parameterServers[kJobs];
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        parameterServers[job] = nodes.Get(psBaseIndex + job);
    }

    PointToPointHelper workerLinks;
    workerLinks.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    workerLinks.SetChannelAttribute("Delay", StringValue("2us"));
    workerLinks.SetQueue("ns3::DropTailQueue", "MaxSize", StringValue("256KB"));

    std::vector<NetDeviceContainer> workerDevices;
    workerDevices.reserve(totalWorkers);
    for (uint32_t worker = 0; worker < totalWorkers; ++worker)
    {
        workerDevices.push_back(
            workerLinks.Install(NodeContainer(nodes.Get(worker), aggregateSwitch)));
    }

    PointToPointHelper psLinks;
    psLinks.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    psLinks.SetChannelAttribute("Delay", StringValue("2us"));
    psLinks.SetQueue("ns3::DropTailQueue", "MaxSize", StringValue("256KB"));

    std::vector<NetDeviceContainer> switchPsDevices;
    Ptr<PointToPointNetDevice> switchPsEgressDevices[kJobs];
    switchPsDevices.reserve(kJobs);
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        switchPsDevices.push_back(
            psLinks.Install(NodeContainer(aggregateSwitch, parameterServers[job])));
        switchPsEgressDevices[job] =
            DynamicCast<PointToPointNetDevice>(switchPsDevices[job].Get(0));
        switchPsEgressDevices[job]->SetThresholdBytes(ecnThresholdBytes);
        switchPsEgressDevices[job]->SetEnableEcnBytes(true);
    }

    InternetStackHelper internet;
    internet.SetRoutingHelper(ATPStaticRoutingHelper());
    internet.Install(nodes);

    Ipv4AddressHelper addresses;
    std::vector<Ipv4InterfaceContainer> workerInterfaces;
    workerInterfaces.reserve(totalWorkers);
    for (uint32_t worker = 0; worker < totalWorkers; ++worker)
    {
        std::ostringstream subnet;
        subnet << "10.1." << (worker + 1) << ".0";
        addresses.SetBase(subnet.str().c_str(), "255.255.255.0");
        workerInterfaces.push_back(addresses.Assign(workerDevices[worker]));
    }
    std::vector<Ipv4InterfaceContainer> switchPsInterfaces;
    Ipv4Address psAddresses[kJobs];
    switchPsInterfaces.reserve(kJobs);
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        std::ostringstream subnet;
        subnet << "10.2." << (job + 1) << ".0";
        addresses.SetBase(subnet.str().c_str(), "255.255.255.0");
        switchPsInterfaces.push_back(addresses.Assign(switchPsDevices[job]));
        psAddresses[job] = switchPsInterfaces[job].GetAddress(1);
    }

    ATPStaticRoutingHelper routingHelper;
    Ptr<ATPStaticRouting> switchRouting =
        routingHelper.GetStaticRouting(aggregateSwitch->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> psRoutings[kJobs];

    switchRouting->SetEnableAggregation(true);
    Ptr<ATPL4Protocol> switchProtocol = switchRouting->GetATPL4Protocol();
    switchProtocol->SetAttribute("MaxAggregators", UintegerValue(aggregatorSlots));
    UintegerValue configuredAggregatorSlots;
    switchProtocol->GetAttribute("MaxAggregators", configuredAggregatorSlots);
    NS_ABORT_MSG_IF(configuredAggregatorSlots.Get() != aggregatorSlots,
                    "Aggregator slot configuration did not take effect");
    switchProtocol->EnableAggregatorCount(true);

    uint32_t switchPsIf[kJobs];
    uint32_t psSwitchIf[kJobs];
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        psRoutings[job] =
            routingHelper.GetStaticRouting(parameterServers[job]->GetObject<Ipv4>());
        switchPsIf[job] =
            InterfaceForDevice(aggregateSwitch, switchPsDevices[job].Get(0));
        psSwitchIf[job] =
            InterfaceForDevice(parameterServers[job], switchPsDevices[job].Get(1));
        switchRouting->AddHostRouteTo(psAddresses[job], psAddresses[job], switchPsIf[job]);
        psRoutings[job]->SetDefaultMulticastRoute(psSwitchIf[job]);
    }

    const Ipv4Address multicastGroups[kJobs] = {Ipv4Address("225.1.1.1"),
                                                Ipv4Address("225.1.1.2"),
                                                Ipv4Address("225.1.1.3"),
                                                Ipv4Address("225.1.1.4")};
    const uint16_t sinkPorts[kJobs] = {9, 10, 11, 12};
    const uint16_t sendPorts[kJobs] = {100, 101, 102, 103};

    Ptr<ATPPacketSink> sinks[kJobs];
    Address sinkAddresses[kJobs];
    std::vector<Ptr<ATPBulkSendApplication>> workerApps;
    std::vector<Ptr<ATPSocket>> workerSockets;
    workerApps.reserve(kJobs * fanin);
    workerSockets.reserve(kJobs * fanin);

    bool workerCountSetterApplied = false;

    Time startTime = Seconds(1.0);
    Time endTime = startTime + MicroSeconds(durationUs);

    for (uint32_t job = 0; job < kJobs; ++job)
    {
        uint8_t jobId = static_cast<uint8_t>(job + 1);
        sinkAddresses[job] = InetSocketAddress(psAddresses[job], sinkPorts[job]);

        sinks[job] = CreateObject<ATPPacketSink>();
        sinks[job]->SetAddressPort(sinkAddresses[job], sinkPorts[job]);
        Ptr<ATPSocket> sinkSocket = DynamicCast<ATPSocket>(
            Socket::CreateSocket(parameterServers[job], ATPSocketFactory::GetTypeId()));
        sinks[job]->SetSocket(sinkSocket);
        NS_ABORT_MSG_IF(sinkSocket->Bind(sinkAddresses[job]) == -1, "Sink bind failed");
        sinkSocket->Listen();
        sinks[job]->SetStartTime(Seconds(0));
        sinks[job]->SetStopTime(endTime);
        parameterServers[job]->AddApplication(sinks[job]);

        uint32_t fullMask = (1u << fanin) - 1u;
        JobTree tree(jobId);
        // This is a single aggregation layer. Keeping fullBitmap1 at zero
        // makes the PS validate bitmap0 instead of treating every bitmap1=1
        // packet as a completed second-layer aggregate.
        tree.fullBitmap1 = 0;
        tree.fanInDegree1 = 1;
        tree.AddBranch(0, static_cast<uint8_t>(fanin), fullMask);
        tree.expectedFlatBitmap = fullMask;
        sinkSocket->SetJobTree(tree);
        sinkSocket->AddAddressMapping(jobId, multicastGroups[job], sendPorts[job]);

        std::vector<uint32_t> multicastOutputs;
        multicastOutputs.reserve(fanin);

        for (uint32_t localWorker = 0; localWorker < fanin; ++localWorker)
        {
            uint32_t workerIndex = job * kProvisionedWorkersPerJob + localWorker;
            Ptr<Node> workerNode = nodes.Get(workerIndex);
            uint32_t workerIf = InterfaceForDevice(workerNode, workerDevices[workerIndex].Get(0));
            uint32_t switchWorkerIf =
                InterfaceForDevice(aggregateSwitch, workerDevices[workerIndex].Get(1));
            Ipv4Address workerAddress = workerInterfaces[workerIndex].GetAddress(0);
            Ipv4Address switchAddress = workerInterfaces[workerIndex].GetAddress(1);

            Ptr<ATPStaticRouting> workerRouting =
                routingHelper.GetStaticRouting(workerNode->GetObject<Ipv4>());
            workerRouting->AddHostRouteTo(psAddresses[job], switchAddress, workerIf);
            switchRouting->AddHostRouteTo(workerAddress, workerAddress, switchWorkerIf);
            psRoutings[job]->AddHostRouteTo(workerAddress,
                                            switchPsInterfaces[job].GetAddress(0),
                                            psSwitchIf[job]);
            multicastOutputs.push_back(switchWorkerIf);

            Ptr<ATPSocket> workerSocket = DynamicCast<ATPSocket>(
                Socket::CreateSocket(workerNode, ATPSocketFactory::GetTypeId()));
            workerSocket->SetInitCwnd(initCwnd);
            workerSocket->SetRetxTimeout(MicroSeconds(500));
            workerSocket->SetRetxCheckInterval(MicroSeconds(100));
            workerCountSetterApplied =
                SetWorkerCount(workerSocket, fanin, 0) || workerCountSetterApplied;

            Ptr<ATPBulkSendApplication> app = CreateObject<ATPBulkSendApplication>();
            app->Setup(sinkAddresses[job], workerSocket, 0, jobId);
            app->SetEnableATPTag(true);
            app->SetFaninDegree0(fanin);
            app->SetFaninDegree1(1);
            app->SetBitmap0(1u << localWorker);
            app->SetBitmap1(1u);

            workerSocket->SetConnectCallback(
                MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, app),
                MakeCallback(&ATPBulkSendApplication::ConnectionFailed, app));
            workerSocket->SetSendCallback(
                MakeCallback(&ATPBulkSendApplication::DataSend, app));
            NS_ABORT_MSG_IF(
                workerSocket->Bind(InetSocketAddress(Ipv4Address::GetAny(), sendPorts[job])) == -1,
                "Worker bind failed");
            workerSocket->Connect(sinkAddresses[job]);
            app->SetStartTime(startTime);
            app->SetStopTime(endTime);
            workerNode->AddApplication(app);

            workerSockets.push_back(workerSocket);
            workerApps.push_back(app);
        }

        switchRouting->AddMulticastRoute(psAddresses[job],
                                         multicastGroups[job],
                                         switchPsIf[job],
                                         multicastOutputs);
    }

    g_measurement = MeasurementState{};
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        g_measurement.sinks[job] = sinks[job];
    }
    g_measurement.protocol = switchProtocol;
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        g_measurement.psEgressDevices[job] = switchPsEgressDevices[job];
    }
    g_measurement.measurementEnd = endTime;
    g_measurement.sampleInterval = MicroSeconds(kSampleIntervalUs);

    Time measurementStart = startTime + MicroSeconds(kWarmupUs);
    Simulator::Schedule(measurementStart, &CaptureWarmupBytes);
    Simulator::Schedule(measurementStart, &SampleResources);
    Simulator::Stop(endTime + MicroSeconds(1));
    Simulator::Run();

    uint64_t finalBytes[kJobs]{};
    double measurementSeconds = static_cast<double>(durationUs - kWarmupUs) / 1e6;
    double goodputMbps[kJobs]{};
    double totalGoodput = 0.0;
    double sumSquaredGoodput = 0.0;
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        finalBytes[job] = sinks[job]->GetTotalRxJob(job + 1);
        goodputMbps[job] =
            (finalBytes[job] - g_measurement.warmupBytes[job]) * 8.0 /
            (measurementSeconds * 1e6);
        totalGoodput += goodputMbps[job];
        sumSquaredGoodput += goodputMbps[job] * goodputMbps[job];
    }
    double denominator = static_cast<double>(kJobs) * sumSquaredGoodput;
    double jainFairness = denominator > 0.0 ? totalGoodput * totalGoodput / denominator : 0.0;

    double occupancyMean[kJobs]{};
    if (g_measurement.occupancySamples > 0)
    {
        for (uint32_t job = 0; job < kJobs; ++job)
        {
            occupancyMean[job] =
                static_cast<double>(g_measurement.occupancySum[job]) /
                g_measurement.occupancySamples;
        }
    }
    double outputQueueMean[kJobs]{};
    double totalOutputQueueMean = 0.0;
    if (g_measurement.queueSamples > 0)
    {
        for (uint32_t job = 0; job < kJobs; ++job)
        {
            outputQueueMean[job] =
                static_cast<double>(g_measurement.queuePacketSum[job]) /
                g_measurement.queueSamples;
        }
        totalOutputQueueMean =
            static_cast<double>(g_measurement.totalQueuePacketSum) /
            g_measurement.queueSamples;
    }

    std::ofstream summary(outputDir + "/summary.txt",
                          std::ofstream::out | std::ofstream::trunc);
    NS_ABORT_MSG_IF(!summary.is_open(), "Cannot open result summary");
    summary << std::fixed << std::setprecision(6)
            << "Algorithm=" << algorithm << "\n"
            << "Jobs=" << kJobs << "\n"
            << "Topology=single-layer-one-shared-aggregator-four-dedicated-ps-links\n"
            << "ProvisionedWorkersPerJob=" << kProvisionedWorkersPerJob << "\n"
            << "WorkersPerJob=" << fanin << "\n"
            << "ActiveWorkersPerJob=" << fanin << "\n"
            << "AggregatorSwitches=1\n"
            << "ParameterServers=4\n"
            << "PsLinks=4\n"
            << "AggregatorSlots=" << aggregatorSlots << "\n"
            << "AggregatorCapacityMode=runtime-set\n"
            << "WorkerCountSetterApplied="
            << (workerCountSetterApplied ? "true" : "false") << "\n"
            << "InitialCwnd=" << initCwnd << "\n"
            << "RetxTimeoutUs=500\n"
            << "RetxCheckIntervalUs=100\n"
            << "WorkerLinkRateGbps=100\n"
            << "PsLinkRateGbps=100\n"
            << "LinkDelayUs=2\n"
            << "QueueBytes=262144\n"
            << "EcnThresholdBytes=" << ecnThresholdBytes << "\n"
            << "Straggler=none\n"
            << "BackgroundTraffic=none\n"
            << "DurationUs=" << durationUs << "\n"
            << "WarmupUs=" << kWarmupUs << "\n";
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        summary << "Job" << (job + 1) << "WarmupBytes="
                << g_measurement.warmupBytes[job] << "\n"
                << "Job" << (job + 1) << "FinalBytes=" << finalBytes[job] << "\n"
                << "Job" << (job + 1) << "GoodputMbps=" << goodputMbps[job] << "\n"
                << "Job" << (job + 1) << "AggregatorOccupancyMean="
                << occupancyMean[job] << "\n"
                << "Job" << (job + 1) << "AggregatorOccupancyMax="
                << g_measurement.occupancyMax[job] << "\n"
                << "Job" << (job + 1) << "OutputQueuePacketsMean="
                << outputQueueMean[job] << "\n"
                << "Job" << (job + 1) << "OutputQueuePacketsMax="
                << g_measurement.queuePacketMax[job] << "\n";
    }
    summary << "TotalGoodputMbps=" << totalGoodput << "\n"
            << "JainFairness=" << jainFairness << "\n"
            << "TotalOutputQueuePacketsMean=" << totalOutputQueueMean << "\n"
            << "TotalOutputQueuePacketsMax=" << g_measurement.totalQueuePacketMax << "\n"
            << "BottleneckQueuePacketsMean=" << totalOutputQueueMean << "\n"
            << "BottleneckQueuePacketsMax=" << g_measurement.totalQueuePacketMax << "\n";
    summary.close();

    std::cout << std::fixed << std::setprecision(3)
              << "RESULT algorithm=" << algorithm << " jobs=" << kJobs
              << " fanin=" << fanin
              << " slots=" << aggregatorSlots
              << " ecn_threshold_bytes=" << ecnThresholdBytes;
    for (uint32_t job = 0; job < kJobs; ++job)
    {
        std::cout << " job" << (job + 1) << "_mbps=" << goodputMbps[job];
    }
    std::cout << " total_mbps=" << totalGoodput
              << " fairness=" << jainFairness
              << " queue_max_total_pkts=" << g_measurement.totalQueuePacketMax << std::endl;

    Simulator::Destroy();
    return 0;
}
