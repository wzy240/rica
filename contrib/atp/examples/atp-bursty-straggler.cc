#include "ns3/applications-module.h"
#include "ns3/atp-module.h"
#include "ns3/core-module.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include "ns3/packet-sink.h"
#include "ns3/point-to-point-module.h"
#include "ns3/ptr.h"

#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>

using namespace ns3;
using namespace std;

NS_LOG_COMPONENT_DEFINE("ATP-Bursty-Straggler");

std::ofstream cwndStream_job1;
std::ofstream cwndStream_job2;
std::ofstream sinkBytesStream_job1;
std::ofstream sinkBytesStream_job2;
std::ofstream aggOccStream;

uint64_t lastTimeJob1Bytes = 0;
uint64_t lastTimeJob2Bytes = 0;

static void
MeasurementRxJob1(Ptr<ATPPacketSink> sink)
{
    Time now = Simulator::Now();
    uint64_t currentTimeJob1Bytes = sink->GetTotalRxJob(1);
    uint64_t rxPer100ms = currentTimeJob1Bytes - lastTimeJob1Bytes;

    sinkBytesStream_job1 << now.GetMicroSeconds() << "\t" << currentTimeJob1Bytes << "\t"
                         << rxPer100ms << std::endl;
    lastTimeJob1Bytes = currentTimeJob1Bytes;
    Simulator::Schedule(MicroSeconds(100), &MeasurementRxJob1, sink);
}

static void
MeasurementRxJob2(Ptr<ATPPacketSink> sink)
{
    Time now = Simulator::Now();
    uint64_t currentTimeJob2Bytes = sink->GetTotalRxJob(2);
    uint64_t rxPer100ms = currentTimeJob2Bytes - lastTimeJob2Bytes;

    sinkBytesStream_job2 << now.GetMicroSeconds() << "\t" << currentTimeJob2Bytes << "\t"
                         << rxPer100ms << std::endl;
    lastTimeJob2Bytes = currentTimeJob2Bytes;
    Simulator::Schedule(MicroSeconds(100), &MeasurementRxJob2, sink);
}

static void
CwndChange_job1(uint32_t oldCwnd, uint32_t newCwnd)
{
    cwndStream_job1 << Simulator::Now().GetMicroSeconds() << "\t" << newCwnd << std::endl;
}

static void
CwndChange_job2(uint32_t oldCwnd, uint32_t newCwnd)
{
    cwndStream_job2 << Simulator::Now().GetMicroSeconds() << "\t" << newCwnd << std::endl;
}

static void
MeasurementAggOccupancy(Ptr<ATPL4Protocol> protocol, uint8_t job1Id, uint8_t job2Id)
{
    Time now = Simulator::Now();
    uint32_t job1Active = protocol->GetAggregatorCount(job1Id);
    uint32_t job2Active = protocol->GetAggregatorCount(job2Id);

    aggOccStream << now.GetMicroSeconds() << "\t" << job1Active << "\t" << job2Active << std::endl;
    Simulator::Schedule(MicroSeconds(50), &MeasurementAggOccupancy, protocol, job1Id, job2Id);
}

int
main(int argc, char* argv[])
{
    int seed = 5; // 设置初始默认值

    CommandLine cmd(__FILE__);
    cmd.AddValue("seed", "Random simulation seed", seed);
    cmd.Parse(argc, argv);

    cwndStream_job1.open("atp-result/trace-bursty/job1-cwnd.txt", std::ofstream::out | std::ofstream::trunc);
    cwndStream_job2.open("atp-result/trace-bursty/job2-cwnd.txt", std::ofstream::out | std::ofstream::trunc);
    sinkBytesStream_job1.open("atp-result/trace-bursty/job1-sinkBytes.txt", std::ofstream::out | std::ofstream::trunc);
    sinkBytesStream_job2.open("atp-result/trace-bursty/job2-sinkBytes.txt", std::ofstream::out | std::ofstream::trunc);
    aggOccStream.open("atp-result/trace-bursty/s0-agg-occupancy.txt", std::ofstream::out | std::ofstream::trunc);

    Time stopTime = Seconds(1.0) + MicroSeconds(20001);
    uint32_t maxBytes = 0;
    uint32_t job1_initCwnd = 32;
    uint32_t job2_initCwnd = 32;

    Ipv4Address multicastGroupJob1("225.1.1.1");
    Ipv4Address multicastGroupJob2("225.1.1.2");

    // 0~3: job1 workers, 4~7: job2 workers, 8:s0, 9:ps, 10:bg-sink
    NodeContainer nodes;
    nodes.Create(11);

    NodeContainer w0s0(nodes.Get(0), nodes.Get(8));
    NodeContainer w1s0(nodes.Get(1), nodes.Get(8));
    NodeContainer w2s0(nodes.Get(2), nodes.Get(8));
    NodeContainer w3s0(nodes.Get(3), nodes.Get(8));

    NodeContainer w4s0(nodes.Get(4), nodes.Get(8));
    NodeContainer w5s0(nodes.Get(5), nodes.Get(8));
    NodeContainer w6s0(nodes.Get(6), nodes.Get(8));
    NodeContainer w7s0(nodes.Get(7), nodes.Get(8));

    NodeContainer s0ps(nodes.Get(8), nodes.Get(9));
    NodeContainer s0bg(nodes.Get(8), nodes.Get(10));

    PointToPointHelper pointToPoint;
    pointToPoint.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    pointToPoint.SetChannelAttribute("Delay", StringValue("2us"));
    pointToPoint.SetQueue("ns3::DropTailQueue", "MaxSize", StringValue("256KB"));

    NetDeviceContainer dev_w0s0 = pointToPoint.Install(w0s0);
    NetDeviceContainer dev_w1s0 = pointToPoint.Install(w1s0);
    NetDeviceContainer dev_w2s0 = pointToPoint.Install(w2s0);
    NetDeviceContainer dev_w3s0 = pointToPoint.Install(w3s0);

    NetDeviceContainer dev_w4s0 = pointToPoint.Install(w4s0);
    NetDeviceContainer dev_w5s0 = pointToPoint.Install(w5s0);
    NetDeviceContainer dev_w6s0 = pointToPoint.Install(w6s0);
    NetDeviceContainer dev_w7s0 = pointToPoint.Install(w7s0);

    NetDeviceContainer dev_s0ps = pointToPoint.Install(s0ps);
    NetDeviceContainer dev_s0bg = pointToPoint.Install(s0bg);

    // ECN 设置
    auto setEcnBytes = [](Ptr<PointToPointNetDevice> dev) {
        dev->SetThresholdBytes(51200); // 50KB => 极大增加 ECN 和重传几率
        dev->SetEnableEcnBytes(true);
    };
    // 给 s0 接收下行到 PS 的出口开 ECN
    setEcnBytes(DynamicCast<PointToPointNetDevice>(dev_s0ps.Get(0)));
    // 给 s0 接收 w0 的入口开 ECN (为了针对背景流量)
    setEcnBytes(DynamicCast<PointToPointNetDevice>(dev_w0s0.Get(1)));

    InternetStackHelper internet;
    internet.SetRoutingHelper(ATPStaticRoutingHelper());
    internet.Install(nodes);

    Ipv4AddressHelper ipv4Helper;
    ipv4Helper.SetBase("10.1.1.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w0s0 = ipv4Helper.Assign(dev_w0s0);
    ipv4Helper.SetBase("10.1.2.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w1s0 = ipv4Helper.Assign(dev_w1s0);
    ipv4Helper.SetBase("10.1.3.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w2s0 = ipv4Helper.Assign(dev_w2s0);
    ipv4Helper.SetBase("10.1.4.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w3s0 = ipv4Helper.Assign(dev_w3s0);

    ipv4Helper.SetBase("10.1.5.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w4s0 = ipv4Helper.Assign(dev_w4s0);
    ipv4Helper.SetBase("10.1.6.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w5s0 = ipv4Helper.Assign(dev_w5s0);
    ipv4Helper.SetBase("10.1.7.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w6s0 = ipv4Helper.Assign(dev_w6s0);
    ipv4Helper.SetBase("10.1.8.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w7s0 = ipv4Helper.Assign(dev_w7s0);

    ipv4Helper.SetBase("10.1.9.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s0ps = ipv4Helper.Assign(dev_s0ps);

    ipv4Helper.SetBase("10.1.10.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s0bg = ipv4Helper.Assign(dev_s0bg);


    // Applications 
    Ptr<ATPPacketSink> sinkApp1 = CreateObject<ATPPacketSink>();
    Ptr<ATPPacketSink> sinkApp2 = CreateObject<ATPPacketSink>();

    uint16_t sinkPort1 = 9, sinkPort2 = 10;
    uint16_t sendPort1 = 11, sendPort2 = 12;
    uint8_t job1Id = 1, job2Id = 2;

    Address sinkAddress1(InetSocketAddress(ip_s0ps.GetAddress(1), sinkPort1));
    Address sinkAddress2(InetSocketAddress(ip_s0ps.GetAddress(1), sinkPort2));

    sinkApp1->SetAddressPort(sinkAddress1, sinkPort1);
    sinkApp2->SetAddressPort(sinkAddress2, sinkPort2);

    Ptr<ATPSocket> sinkATPSocket1 = DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(9), ATPSocketFactory::GetTypeId()));
    Ptr<ATPSocket> sinkATPSocket2 = DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(9), ATPSocketFactory::GetTypeId()));

    sinkApp1->SetSocket(sinkATPSocket1);
    sinkATPSocket1->Bind(sinkAddress1);
    sinkATPSocket1->Listen();

    sinkApp2->SetSocket(sinkATPSocket2);
    sinkATPSocket2->Bind(sinkAddress2);
    sinkATPSocket2->Listen();

    sinkApp1->SetStartTime(Seconds(0.0));
    sinkApp1->SetStopTime(stopTime);
    nodes.Get(9)->AddApplication(sinkApp1);

    sinkApp2->SetStartTime(Seconds(0.0));
    sinkApp2->SetStopTime(stopTime);
    nodes.Get(9)->AddApplication(sinkApp2);

    std::vector<Ptr<ATPSocket>> workerSockets;
    for (int i = 0; i < 8; ++i) {
        auto sock = DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(i), ATPSocketFactory::GetTypeId()));
        uint32_t initCwnd = (i < 4) ? job1_initCwnd : job2_initCwnd;
        sock->SetInitCwnd(initCwnd);
        sock->SetRetxTimeout(MicroSeconds(500)); // 超时重传
        sock->SetRetxCheckInterval(MicroSeconds(100));
        workerSockets.push_back(sock);
    }

    std::vector<Ptr<ATPBulkSendApplication>> workerApps;
    for (int i = 0; i < 8; ++i) {
        auto app = CreateObject<ATPBulkSendApplication>();
        uint8_t jobId = (i < 4) ? job1Id : job2Id;
        Address sinkAddr = (i < 4) ? sinkAddress1 : sinkAddress2;

        app->Setup(sinkAddr, workerSockets[i], maxBytes, jobId);
        app->SetEnableATPTag(true);
        app->SetFaninDegree0(4);
        app->SetFaninDegree1(1);
        
        uint32_t bitmap0 = 1 << (i % 4); 
        app->SetBitmap0(bitmap0);
        app->SetBitmap1(0b01);
        
        workerSockets[i]->SetConnectCallback(MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, app), MakeCallback(&ATPBulkSendApplication::ConnectionFailed, app));
        workerSockets[i]->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, app));
        
        uint16_t sendPort = (i < 4) ? sendPort1 : sendPort2;
        workerSockets[i]->Bind(InetSocketAddress(Ipv4Address::GetAny(), sendPort));
        workerSockets[i]->Connect(sinkAddr);

        app->SetStartTime(Seconds(1.0));
        app->SetStopTime(stopTime);
        nodes.Get(i)->AddApplication(app);
        
        workerApps.push_back(app);
    }

    workerSockets[0]->GetTxBuffer()->TraceConnectWithoutContext("cwndTrace", MakeCallback(&CwndChange_job1));
    workerSockets[4]->GetTxBuffer()->TraceConnectWithoutContext("cwndTrace", MakeCallback(&CwndChange_job2));

    // Routing
    ATPStaticRoutingHelper staticRoutingHelper;
    Ptr<ATPStaticRouting> r_s0 = staticRoutingHelper.GetStaticRouting(nodes.Get(8)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> r_ps = staticRoutingHelper.GetStaticRouting(nodes.Get(9)->GetObject<Ipv4>());
    
    r_s0->SetEnableAggregation(true);
    r_s0->GetATPL4Protocol()->EnableAggregatorCount(true);

    JobTree job1Tree(job1Id);
    job1Tree.fullBitmap1 = 0b00;
    job1Tree.fanInDegree1 = 1;
    job1Tree.AddBranch(0, 4, 0b1111);
    job1Tree.expectedFlatBitmap = 0b1111;
    sinkATPSocket1->SetJobTree(job1Tree);

    JobTree job2Tree(job2Id);
    job2Tree.fullBitmap1 = 0b00;
    job2Tree.fanInDegree1 = 1;
    job2Tree.AddBranch(0, 4, 0b1111);
    job2Tree.expectedFlatBitmap = 0b1111;
    sinkATPSocket2->SetJobTree(job2Tree);

    staticRoutingHelper.GetStaticRouting(nodes.Get(0)->GetObject<Ipv4>())
        ->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w0s0.GetAddress(1), 1);
    staticRoutingHelper.GetStaticRouting(nodes.Get(1)->GetObject<Ipv4>())
        ->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w1s0.GetAddress(1), 1);
    staticRoutingHelper.GetStaticRouting(nodes.Get(2)->GetObject<Ipv4>())
        ->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w2s0.GetAddress(1), 1);
    staticRoutingHelper.GetStaticRouting(nodes.Get(3)->GetObject<Ipv4>())
        ->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w3s0.GetAddress(1), 1);

    staticRoutingHelper.GetStaticRouting(nodes.Get(4)->GetObject<Ipv4>())
        ->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w4s0.GetAddress(1), 1);
    staticRoutingHelper.GetStaticRouting(nodes.Get(5)->GetObject<Ipv4>())
        ->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w5s0.GetAddress(1), 1);
    staticRoutingHelper.GetStaticRouting(nodes.Get(6)->GetObject<Ipv4>())
        ->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w6s0.GetAddress(1), 1);
    staticRoutingHelper.GetStaticRouting(nodes.Get(7)->GetObject<Ipv4>())
        ->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w7s0.GetAddress(1), 1);

    r_s0->AddMulticastRoute(ip_s0ps.GetAddress(1), multicastGroupJob1, 9, {1, 2, 3, 4});
    r_s0->AddMulticastRoute(ip_s0ps.GetAddress(1), multicastGroupJob2, 9, {5, 6, 7, 8});

    // 收集所有 worker 的本地 IP 地址 (即它们对应的接口容器的第 0 个接口的 IP)
    std::vector<Ipv4Address> workerIps = {
        ip_w0s0.GetAddress(0), ip_w1s0.GetAddress(0), ip_w2s0.GetAddress(0), ip_w3s0.GetAddress(0),
        ip_w4s0.GetAddress(0), ip_w5s0.GetAddress(0), ip_w6s0.GetAddress(0), ip_w7s0.GetAddress(0)
    };

    // 配置 s0 和 PS 去往各个 worker 的下行路由
    for (int i = 0; i < 8; ++i)
    {
        Ipv4Address nodeAddr = workerIps[i];
        // s0 去往 worker_i 的包，直接从 s0 对应的第 i+1 个接口发出 (因为 0是环回，1开始是依次分配的P2P接口)
        r_s0->AddHostRouteTo(nodeAddr, nodeAddr, i + 1);
        // PS 去往 worker_i 的包，下一跳都是 s0ps 链路中 s0 的 IP
        r_ps->AddHostRouteTo(nodeAddr, ip_s0ps.GetAddress(0), 1);
        r_ps->SetDefaultMulticastRoute(1);
    }

    r_s0->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_s0ps.GetAddress(1), 9);
    r_s0->AddHostRouteTo(ip_s0bg.GetAddress(1), ip_s0bg.GetAddress(1), 10);
    r_ps->SetDefaultMulticastRoute(1);

    sinkATPSocket1->AddAddressMapping(job1Id, multicastGroupJob1, sendPort1);
    sinkATPSocket2->AddAddressMapping(job2Id, multicastGroupJob2, sendPort2);


    // ==========================================
    // 模拟阵发性网络突发干扰 (Bursty Traffic) - 泊松大象流
    // ==========================================
    RngSeedManager::SetSeed(seed); // 设置新的种子
    uint16_t bgPort = 5000;
    PacketSinkHelper bgSinkHelper("ns3::UdpSocketFactory", InetSocketAddress(Ipv4Address::GetAny(), bgPort));
    ApplicationContainer bgSinks = bgSinkHelper.Install(nodes.Get(10));
    
    // w0 到 bg_receiver 路由
    staticRoutingHelper.GetStaticRouting(nodes.Get(0)->GetObject<Ipv4>())->AddHostRouteTo(ip_s0bg.GetAddress(1), ip_w0s0.GetAddress(1), 1);

    OnOffHelper bgSourceHelper("ns3::UdpSocketFactory", Address(InetSocketAddress(ip_s0bg.GetAddress(1), bgPort)));
    
    // 1. 设置发送速率为 30Gbps (与 v1 单源一致), 包大小 1500
    bgSourceHelper.SetAttribute("DataRate", StringValue("30Gbps"));
    bgSourceHelper.SetAttribute("PacketSize", UintegerValue(1500));
    
    // 2. 保证流一旦启动就能一直发，直到发满设置的 MaxBytes
    bgSourceHelper.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=1.0]"));
    bgSourceHelper.SetAttribute("OffTime", StringValue("ns3::ConstantRandomVariable[Constant=0.0]"));

    // 3. 设定每个大象流的大小，例如 5 MB。发送 5MB 在 30Gbps 下耗时约 1.3ms
    uint32_t elephantSizeBytes = 5 * 1024 * 1024;
    bgSourceHelper.SetAttribute("MaxBytes", UintegerValue(elephantSizeBytes));

    ApplicationContainer bgSources;

    // 使用指数分布随机变量生成泊松到达的间隔时间
    Ptr<ExponentialRandomVariable> expRand = CreateObject<ExponentialRandomVariable>();
    // 泊松到达的平均间隔时间
    expRand->SetAttribute("Mean", DoubleValue(0.002));

    int numElephantFlows = 100;    // 因为单个流很短，提升生成的总大象流数以覆盖整个仿真周期(1.0s stop time)
    double currentStartTime = 1.0; // 基础起始时间

    for (int i = 0; i < numElephantFlows; ++i)
    {
        // 保证在仿真结束前才加入流
        if (currentStartTime > stopTime.GetSeconds()) {
            break;
        }

        ApplicationContainer flowApp = bgSourceHelper.Install(nodes.Get(0));

        // 按照指数分布累加生成下一次的开始时间
        flowApp.Start(Seconds(currentStartTime));
        flowApp.Stop(stopTime);
        bgSources.Add(flowApp);

        // 累加时间间隔以便调度下一条流
        currentStartTime += expRand->GetValue();
    }

    bgSinks.Start(Seconds(0.0));
    bgSinks.Stop(stopTime);


    // Measurements
    Simulator::Schedule(Seconds(1.0), &MeasurementRxJob1, sinkApp1);
    Simulator::Schedule(Seconds(1.0), &MeasurementRxJob2, sinkApp2);
    Simulator::Schedule(Seconds(1.0), &MeasurementAggOccupancy, r_s0->GetATPL4Protocol(), job1Id, job2Id);

    NS_LOG_INFO("Run Simulation.");
    Simulator::Stop(stopTime);
    Simulator::Run();
    Simulator::Destroy();
    NS_LOG_INFO("Done.");

    cwndStream_job1.close();
    cwndStream_job2.close();
    sinkBytesStream_job1.close();
    sinkBytesStream_job2.close();
    aggOccStream.close();

    std::cout << "job1 Total Received: " << sinkApp1->GetTotalRxJob(job1Id) << std::endl;
    std::cout << "job2 Total Received: " << sinkApp2->GetTotalRxJob(job2Id) << std::endl;

    return 0;
}
