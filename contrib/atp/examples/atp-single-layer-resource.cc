#include "ns3/applications-module.h"
#include "ns3/atp-module.h" // atp module
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

NS_LOG_COMPONENT_DEFINE("ATP-Single-Layer-Aggregation");

// 拥塞窗口跟踪，写入txt文件
std::ofstream cwndStream_job1;
std::ofstream cwndStream_job2;

// PacketSink端接收到的job1和job2的字节数
uint64_t lastTimeJob1Bytes = 0;
uint64_t lastTimeJob2Bytes = 0;

std::ofstream sinkBytesStream_job1;
std::ofstream sinkBytesStream_job2;
std::ofstream aggOccStream;

// 记录接收端接收的总字节数
static void
MeasurementRxJob1(Ptr<ATPPacketSink> sink)
{
    Time now = Simulator::Now();

    uint64_t currentTimeJob1Bytes = sink->GetTotalRxJob(1); // job1

    // 100us内接收的字节数
    uint64_t ReceiveJob1BytesPer100ms = currentTimeJob1Bytes - lastTimeJob1Bytes;

    // 记录总字节数和本100us内接收的字节数
    sinkBytesStream_job1 << now.GetMicroSeconds() << "\t" << currentTimeJob1Bytes << "\t"
                         << ReceiveJob1BytesPer100ms << std::endl;

    lastTimeJob1Bytes = currentTimeJob1Bytes;

    // 调度下一个测量
    Simulator::Schedule(MicroSeconds(100), &MeasurementRxJob1, sink);
}

static void
MeasurementRxJob2(Ptr<ATPPacketSink> sink)
{
    Time now = Simulator::Now();

    uint64_t currentTimeJob2Bytes = sink->GetTotalRxJob(2); // job2

    // 100us内接收到的字节数
    uint64_t ReceiveJob2BytesPer100ms = currentTimeJob2Bytes - lastTimeJob2Bytes;

    // 记录总字节数和本100us内接收的字节数
    sinkBytesStream_job2 << now.GetMicroSeconds() << "\t" << currentTimeJob2Bytes << "\t"
                         << ReceiveJob2BytesPer100ms << std::endl;

    lastTimeJob2Bytes = currentTimeJob2Bytes;

    // 调度下一个测量
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

    Simulator::Schedule(MicroSeconds(20), &MeasurementAggOccupancy, protocol, job1Id, job2Id);
}

int
main(int argc, char* argv[])
{
    // LogComponentEnable("ATPSocket", LOG_LEVEL_ALL);
    // LogComponentEnable("ATPL4Protocol", LOG_LEVEL_INFO);

    cwndStream_job1.open("atp-result/trace-sl-resource/job1-cwnd.txt",
                         std::ofstream::out | std::ofstream::trunc);
    cwndStream_job2.open("atp-result/trace-sl-resource/job2-cwnd.txt",
                         std::ofstream::out | std::ofstream::trunc);
    sinkBytesStream_job1.open("atp-result/trace-sl-resource/job1-sinkBytes.txt",
                              std::ofstream::out | std::ofstream::trunc);
    sinkBytesStream_job2.open("atp-result/trace-sl-resource/job2-sinkBytes.txt",
                              std::ofstream::out | std::ofstream::trunc);
    aggOccStream.open("atp-result/trace-sl-resource/s0-agg-occupancy.txt",
                      std::ofstream::out | std::ofstream::trunc);

    uint32_t maxBytes = 0;
    Time stopTime = Seconds(1.0) + MicroSeconds(20001);

    uint64_t initialTimestamp = 1000000;
    uint32_t job1_initCwnd = 1;
    uint32_t job2_initCwnd = 1;

    Ipv4Address multicastGroupJob1("225.1.1.1");
    Ipv4Address multicastGroupJob2("225.1.1.2");

    // straggler 背景流参数
    int32_t job1StragglerId = -1; // baseline: no job1 background flow
    int32_t job2StragglerId = -4; // job2: w4
    std::string bgDataRate = "0Gbps";
    uint32_t maxAggregators = 2048;

    CommandLine cmd;
    cmd.AddValue("maxAggregators", "Maximum number of aggregator slots on s0", maxAggregators);
    cmd.Parse(argc, argv);

    std::cout << "Job1 Straggler: w" << job1StragglerId << std::endl;
    std::cout << "Job2 Straggler: w" << job2StragglerId << std::endl;
    std::cout << "bgDataRate: " << bgDataRate << std::endl;
    std::cout << "maxAggregators: " << maxAggregators << std::endl;

    cwndStream_job1 << initialTimestamp << "\t" << job1_initCwnd << std::endl;
    cwndStream_job2 << initialTimestamp << "\t" << job2_initCwnd << std::endl;

    //
    // 简化单层拓扑（全 P2P）:
    // 0~3: job1 workers, 4~7: job2 workers, 8:s0, 9:ps, 10:bg1, 11:bg2
    //
    NS_LOG_INFO("Create nodes.");
    NodeContainer nodes;
    nodes.Create(12);

    NodeContainer w0s0(nodes.Get(0), nodes.Get(8));
    NodeContainer w1s0(nodes.Get(1), nodes.Get(8));
    NodeContainer w2s0(nodes.Get(2), nodes.Get(8));
    NodeContainer w3s0(nodes.Get(3), nodes.Get(8));

    NodeContainer w4s0(nodes.Get(4), nodes.Get(8));
    NodeContainer w5s0(nodes.Get(5), nodes.Get(8));
    NodeContainer w6s0(nodes.Get(6), nodes.Get(8));
    NodeContainer w7s0(nodes.Get(7), nodes.Get(8));

    NodeContainer s0ps(nodes.Get(8), nodes.Get(9));
    NodeContainer s0bg1(nodes.Get(8), nodes.Get(10));
    NodeContainer s0bg2(nodes.Get(8), nodes.Get(11));

    NS_LOG_INFO("Create channels.");
    PointToPointHelper pointToPoint;
    pointToPoint.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    pointToPoint.SetChannelAttribute("Delay", StringValue("2us"));
    pointToPoint.SetQueue("ns3::DropTailQueue", "MaxSize", StringValue("256KB"));

    // worker<->s0（全部统一 100Gbps）
    NetDeviceContainer dev_w0s0 = pointToPoint.Install(w0s0);
    NetDeviceContainer dev_w1s0 = pointToPoint.Install(w1s0);
    NetDeviceContainer dev_w2s0 = pointToPoint.Install(w2s0);
    NetDeviceContainer dev_w3s0 = pointToPoint.Install(w3s0);

    NetDeviceContainer dev_w4s0 = pointToPoint.Install(w4s0);
    NetDeviceContainer dev_w5s0 = pointToPoint.Install(w5s0);
    NetDeviceContainer dev_w6s0 = pointToPoint.Install(w6s0);
    NetDeviceContainer dev_w7s0 = pointToPoint.Install(w7s0);

    // s0<->ps, s0<->bg
    NetDeviceContainer dev_s0ps = pointToPoint.Install(s0ps);
    NetDeviceContainer dev_s0bg1 = pointToPoint.Install(s0bg1);
    NetDeviceContainer dev_s0bg2 = pointToPoint.Install(s0bg2);

    // s0 侧端口阈值和 ECN（按字节）
    auto setEcnBytes = [](Ptr<PointToPointNetDevice> dev) {
        dev->SetThresholdBytes(32768);
        dev->SetEnableEcnBytes(true);
    };

    setEcnBytes(DynamicCast<PointToPointNetDevice>(dev_s0ps.Get(0)));

    InternetStackHelper internet;
    internet.SetRoutingHelper(ATPStaticRoutingHelper());
    internet.Install(nodes);

    NS_LOG_INFO("Assign IP Addresses.");
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
    Ipv4InterfaceContainer ip_s0bg1 = ipv4Helper.Assign(dev_s0bg1);
    ipv4Helper.SetBase("10.1.11.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s0bg2 = ipv4Helper.Assign(dev_s0bg2);

    NS_LOG_INFO("Create Applications.");
    Ptr<ATPPacketSink> sinkApp1 = CreateObject<ATPPacketSink>();
    Ptr<ATPPacketSink> sinkApp2 = CreateObject<ATPPacketSink>();

    uint16_t sinkPort1 = 9;
    uint16_t sinkPort2 = 10;
    uint16_t sendPort1 = 11;
    uint16_t sendPort2 = 12;
    uint8_t job1Id = 1;
    uint8_t job2Id = 2;

    Address sinkAddress1(InetSocketAddress(ip_s0ps.GetAddress(1), sinkPort1));
    Address sinkAddress2(InetSocketAddress(ip_s0ps.GetAddress(1), sinkPort2));

    sinkApp1->SetAddressPort(sinkAddress1, sinkPort1);
    sinkApp2->SetAddressPort(sinkAddress2, sinkPort2);

    Ptr<ATPSocket> sinkATPSocket1 =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(9), ATPSocketFactory::GetTypeId()));
    Ptr<ATPSocket> sinkATPSocket2 =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(9), ATPSocketFactory::GetTypeId()));

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

    // workers socket
    Ptr<ATPSocket> w0Socket =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(0), ATPSocketFactory::GetTypeId()));
    Ptr<ATPSocket> w1Socket =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(1), ATPSocketFactory::GetTypeId()));
    Ptr<ATPSocket> w2Socket =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(2), ATPSocketFactory::GetTypeId()));
    Ptr<ATPSocket> w3Socket =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(3), ATPSocketFactory::GetTypeId()));

    Ptr<ATPSocket> w4Socket =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(4), ATPSocketFactory::GetTypeId()));
    Ptr<ATPSocket> w5Socket =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(5), ATPSocketFactory::GetTypeId()));
    Ptr<ATPSocket> w6Socket =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(6), ATPSocketFactory::GetTypeId()));
    Ptr<ATPSocket> w7Socket =
        DynamicCast<ATPSocket>(Socket::CreateSocket(nodes.Get(7), ATPSocketFactory::GetTypeId()));

    Ptr<ATPBulkSendApplication> app_w0 = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> app_w1 = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> app_w2 = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> app_w3 = CreateObject<ATPBulkSendApplication>();

    Ptr<ATPBulkSendApplication> app_w4 = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> app_w5 = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> app_w6 = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> app_w7 = CreateObject<ATPBulkSendApplication>();

    w0Socket->SetInitCwnd(job1_initCwnd);
    w1Socket->SetInitCwnd(job1_initCwnd);
    w2Socket->SetInitCwnd(job1_initCwnd);
    w3Socket->SetInitCwnd(job1_initCwnd);

    w4Socket->SetInitCwnd(job2_initCwnd);
    w5Socket->SetInitCwnd(job2_initCwnd);
    w6Socket->SetInitCwnd(job2_initCwnd);
    w7Socket->SetInitCwnd(job2_initCwnd);

    // job1: 4 workers
    app_w0->Setup(sinkAddress1, w0Socket, maxBytes, job1Id);
    app_w0->SetEnableATPTag(true);
    app_w0->SetFaninDegree0(4);
    app_w0->SetFaninDegree1(1);
    app_w0->SetBitmap0(0b0001);
    app_w0->SetBitmap1(0b01);

    app_w1->Setup(sinkAddress1, w1Socket, maxBytes, job1Id);
    app_w1->SetEnableATPTag(true);
    app_w1->SetFaninDegree0(4);
    app_w1->SetFaninDegree1(1);
    app_w1->SetBitmap0(0b0010);
    app_w1->SetBitmap1(0b01);

    app_w2->Setup(sinkAddress1, w2Socket, maxBytes, job1Id);
    app_w2->SetEnableATPTag(true);
    app_w2->SetFaninDegree0(4);
    app_w2->SetFaninDegree1(1);
    app_w2->SetBitmap0(0b0100);
    app_w2->SetBitmap1(0b01);

    app_w3->Setup(sinkAddress1, w3Socket, maxBytes, job1Id);
    app_w3->SetEnableATPTag(true);
    app_w3->SetFaninDegree0(4);
    app_w3->SetFaninDegree1(1);
    app_w3->SetBitmap0(0b1000);
    app_w3->SetBitmap1(0b01);

    // job2: 4 workers
    app_w4->Setup(sinkAddress2, w4Socket, maxBytes, job2Id);
    app_w4->SetEnableATPTag(true);
    app_w4->SetFaninDegree0(4);
    app_w4->SetFaninDegree1(1);
    app_w4->SetBitmap0(0b0001);
    app_w4->SetBitmap1(0b01);

    app_w5->Setup(sinkAddress2, w5Socket, maxBytes, job2Id);
    app_w5->SetEnableATPTag(true);
    app_w5->SetFaninDegree0(4);
    app_w5->SetFaninDegree1(1);
    app_w5->SetBitmap0(0b0010);
    app_w5->SetBitmap1(0b01);

    app_w6->Setup(sinkAddress2, w6Socket, maxBytes, job2Id);
    app_w6->SetEnableATPTag(true);
    app_w6->SetFaninDegree0(4);
    app_w6->SetFaninDegree1(1);
    app_w6->SetBitmap0(0b0100);
    app_w6->SetBitmap1(0b01);

    app_w7->Setup(sinkAddress2, w7Socket, maxBytes, job2Id);
    app_w7->SetEnableATPTag(true);
    app_w7->SetFaninDegree0(4);
    app_w7->SetFaninDegree1(1);
    app_w7->SetBitmap0(0b1000);
    app_w7->SetBitmap1(0b01);

    auto installApp = [](Ptr<Node> node,
                         Ptr<ATPBulkSendApplication> app,
                         Ptr<ATPSocket> sock,
                         Address addr,
                         Address sink,
                         Time start,
                         Time stop) {
        sock->SetConnectCallback(MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, app),
                                 MakeCallback(&ATPBulkSendApplication::ConnectionFailed, app));
        sock->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, app));
        sock->Bind(addr);
        sock->Connect(sink);
        app->SetStartTime(start);
        app->SetStopTime(stop);
        node->AddApplication(app);
    };

    installApp(nodes.Get(0),
               app_w0,
               w0Socket,
               InetSocketAddress(Ipv4Address::GetAny(), sendPort1),
               sinkAddress1,
               Seconds(1.0),
               stopTime);
    installApp(nodes.Get(1),
               app_w1,
               w1Socket,
               InetSocketAddress(Ipv4Address::GetAny(), sendPort1),
               sinkAddress1,
               Seconds(1.0),
               stopTime);
    installApp(nodes.Get(2),
               app_w2,
               w2Socket,
               InetSocketAddress(Ipv4Address::GetAny(), sendPort1),
               sinkAddress1,
               Seconds(1.0),
               stopTime);
    installApp(nodes.Get(3),
               app_w3,
               w3Socket,
               InetSocketAddress(Ipv4Address::GetAny(), sendPort1),
               sinkAddress1,
               Seconds(1.0),
               stopTime);

    installApp(nodes.Get(4),
               app_w4,
               w4Socket,
               InetSocketAddress(Ipv4Address::GetAny(), sendPort2),
               sinkAddress2,
               Seconds(1.0),
               stopTime);
    installApp(nodes.Get(5),
               app_w5,
               w5Socket,
               InetSocketAddress(Ipv4Address::GetAny(), sendPort2),
               sinkAddress2,
               Seconds(1.0),
               stopTime);
    installApp(nodes.Get(6),
               app_w6,
               w6Socket,
               InetSocketAddress(Ipv4Address::GetAny(), sendPort2),
               sinkAddress2,
               Seconds(1.0),
               stopTime);
    installApp(nodes.Get(7),
               app_w7,
               w7Socket,
               InetSocketAddress(Ipv4Address::GetAny(), sendPort2),
               sinkAddress2,
               Seconds(1.0),
               stopTime);

    w0Socket->GetTxBuffer()->TraceConnectWithoutContext("cwndTrace", MakeCallback(&CwndChange_job1));
    w4Socket->GetTxBuffer()->TraceConnectWithoutContext("cwndTrace", MakeCallback(&CwndChange_job2));

    // 路由和聚合配置
    ATPStaticRoutingHelper staticRoutingHelper;
    Ptr<ATPStaticRouting> routing_w0 = staticRoutingHelper.GetStaticRouting(nodes.Get(0)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> routing_w1 = staticRoutingHelper.GetStaticRouting(nodes.Get(1)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> routing_w2 = staticRoutingHelper.GetStaticRouting(nodes.Get(2)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> routing_w3 = staticRoutingHelper.GetStaticRouting(nodes.Get(3)->GetObject<Ipv4>());

    Ptr<ATPStaticRouting> routing_w4 = staticRoutingHelper.GetStaticRouting(nodes.Get(4)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> routing_w5 = staticRoutingHelper.GetStaticRouting(nodes.Get(5)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> routing_w6 = staticRoutingHelper.GetStaticRouting(nodes.Get(6)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> routing_w7 = staticRoutingHelper.GetStaticRouting(nodes.Get(7)->GetObject<Ipv4>());

    Ptr<ATPStaticRouting> routing_s0 = staticRoutingHelper.GetStaticRouting(nodes.Get(8)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> routing_ps = staticRoutingHelper.GetStaticRouting(nodes.Get(9)->GetObject<Ipv4>());

    routing_s0->SetEnableAggregation(true);
    routing_s0->GetATPL4Protocol()->SetMaxAggregators(maxAggregators);
    routing_s0->GetATPL4Protocol()->EnableAggregatorCount(true);

    JobTree job1Tree(job1Id);
    job1Tree.fullBitmap1 = 0b00;    // != bitmap1, 符合 socket/ConvertToFlatBitmap 聚合逻辑
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

    // workers -> ps
    routing_w0->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w0s0.GetAddress(1), 1);
    routing_w1->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w1s0.GetAddress(1), 1);
    routing_w2->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w2s0.GetAddress(1), 1);
    routing_w3->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w3s0.GetAddress(1), 1);

    routing_w4->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w4s0.GetAddress(1), 1);
    routing_w5->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w5s0.GetAddress(1), 1);
    routing_w6->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w6s0.GetAddress(1), 1);
    routing_w7->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_w7s0.GetAddress(1), 1);

    // s0 下行路由
    routing_s0->AddMulticastRoute(ip_s0ps.GetAddress(1), multicastGroupJob1, 9, {1, 2, 3, 4});
    routing_s0->AddMulticastRoute(ip_s0ps.GetAddress(1), multicastGroupJob2, 9, {5, 6, 7, 8});

    routing_s0->AddHostRouteTo(ip_w0s0.GetAddress(0), ip_w0s0.GetAddress(0), 1);
    routing_s0->AddHostRouteTo(ip_w1s0.GetAddress(0), ip_w1s0.GetAddress(0), 2);
    routing_s0->AddHostRouteTo(ip_w2s0.GetAddress(0), ip_w2s0.GetAddress(0), 3);
    routing_s0->AddHostRouteTo(ip_w3s0.GetAddress(0), ip_w3s0.GetAddress(0), 4);

    routing_s0->AddHostRouteTo(ip_w4s0.GetAddress(0), ip_w4s0.GetAddress(0), 5);
    routing_s0->AddHostRouteTo(ip_w5s0.GetAddress(0), ip_w5s0.GetAddress(0), 6);
    routing_s0->AddHostRouteTo(ip_w6s0.GetAddress(0), ip_w6s0.GetAddress(0), 7);
    routing_s0->AddHostRouteTo(ip_w7s0.GetAddress(0), ip_w7s0.GetAddress(0), 8);

    routing_s0->AddHostRouteTo(ip_s0ps.GetAddress(1), ip_s0ps.GetAddress(1), 9);
    routing_s0->AddHostRouteTo(ip_s0bg1.GetAddress(1), ip_s0bg1.GetAddress(1), 10);
    routing_s0->AddHostRouteTo(ip_s0bg2.GetAddress(1), ip_s0bg2.GetAddress(1), 11);

    // ps 回程
    routing_ps->SetDefaultMulticastRoute(1);
    routing_ps->AddHostRouteTo(ip_w0s0.GetAddress(0), ip_s0ps.GetAddress(0), 1);
    routing_ps->AddHostRouteTo(ip_w1s0.GetAddress(0), ip_s0ps.GetAddress(0), 1);
    routing_ps->AddHostRouteTo(ip_w2s0.GetAddress(0), ip_s0ps.GetAddress(0), 1);
    routing_ps->AddHostRouteTo(ip_w3s0.GetAddress(0), ip_s0ps.GetAddress(0), 1);
    routing_ps->AddHostRouteTo(ip_w4s0.GetAddress(0), ip_s0ps.GetAddress(0), 1);
    routing_ps->AddHostRouteTo(ip_w5s0.GetAddress(0), ip_s0ps.GetAddress(0), 1);
    routing_ps->AddHostRouteTo(ip_w6s0.GetAddress(0), ip_s0ps.GetAddress(0), 1);
    routing_ps->AddHostRouteTo(ip_w7s0.GetAddress(0), ip_s0ps.GetAddress(0), 1);

    // 组播地址映射
    sinkATPSocket1->AddAddressMapping(job1Id, multicastGroupJob1, sendPort1);
    sinkATPSocket2->AddAddressMapping(job2Id, multicastGroupJob2, sendPort2);

    //
    // 背景流量（straggler）
    //
    uint16_t bgPort = 5000;
    PacketSinkHelper bgSinkHelper("ns3::UdpSocketFactory",
                                  InetSocketAddress(Ipv4Address::GetAny(), bgPort));

    OnOffHelper bgSourceHelper("ns3::UdpSocketFactory", Address());
    bgSourceHelper.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=1]"));
    bgSourceHelper.SetAttribute("OffTime", StringValue("ns3::ConstantRandomVariable[Constant=0]"));
    bgSourceHelper.SetAttribute("DataRate", StringValue(bgDataRate));
    bgSourceHelper.SetAttribute("PacketSize", UintegerValue(1500));

    ApplicationContainer bgSinks;
    ApplicationContainer bgSources;

    if (job1StragglerId == 0)
    {
        bgSinks.Add(bgSinkHelper.Install(nodes.Get(10)));
        routing_w0->AddHostRouteTo(ip_s0bg1.GetAddress(1), ip_w0s0.GetAddress(1), 1);

        Address bgDest1(InetSocketAddress(ip_s0bg1.GetAddress(1), bgPort));
        bgSourceHelper.SetAttribute("Remote", AddressValue(bgDest1));
        bgSources.Add(bgSourceHelper.Install(nodes.Get(0)));
    }

    if (job2StragglerId == 4)
    {
        bgSinks.Add(bgSinkHelper.Install(nodes.Get(11)));
        routing_w4->AddHostRouteTo(ip_s0bg2.GetAddress(1), ip_w4s0.GetAddress(1), 1);

        Address bgDest2(InetSocketAddress(ip_s0bg2.GetAddress(1), bgPort));
        bgSourceHelper.SetAttribute("Remote", AddressValue(bgDest2));
        bgSources.Add(bgSourceHelper.Install(nodes.Get(4)));
    }

    bgSinks.Start(Seconds(0.0));
    bgSinks.Stop(stopTime);
    bgSources.Start(Seconds(0.5));
    bgSources.Stop(stopTime);

    // 发送测量
    Simulator::Schedule(Seconds(1.0), &MeasurementRxJob1, sinkApp1);
    Simulator::Schedule(Seconds(1.0), &MeasurementRxJob2, sinkApp2);
    Simulator::Schedule(Seconds(1.0),
                        &MeasurementAggOccupancy,
                        routing_s0->GetATPL4Protocol(),
                        job1Id,
                        job2Id);

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

    std::cout << "job1 Bytes Sent: " << w0Socket->GetTotalTxBytes() << ", "
              << w1Socket->GetTotalTxBytes() << ", " << w2Socket->GetTotalTxBytes() << ", "
              << w3Socket->GetTotalTxBytes() << std::endl;
    std::cout << "job2 Bytes Sent: " << w4Socket->GetTotalTxBytes() << ", "
              << w5Socket->GetTotalTxBytes() << ", " << w6Socket->GetTotalTxBytes() << ", "
              << w7Socket->GetTotalTxBytes() << std::endl;
    std::cout << "job1 Total Received: " << sinkApp1->GetTotalRxJob(job1Id) << std::endl;
    std::cout << "job2 Total Received: " << sinkApp2->GetTotalRxJob(job2Id) << std::endl;

    return 0;
}
