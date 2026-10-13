/*
 * SPDX-License-Identifier: GPL-2.0-only
 */

// Network topology
// w0,w1,w2(job1) to s0, w3,w4(job1),w5,w6(job2) to s1, and w7,w8,w9(job2) to s2. s0,s1,s2 to n3
//
// - Flow from workers to edge switches using BulkSendApplication.
// - Tracing of queues and packet receptions to file "tcp-bulk-send.tr"
//   and pcap tracing available when tracing is turned on

#include "ns3/applications-module.h"
#include "ns3/atp-module.h" // atp module
#include "ns3/core-module.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include "ns3/packet-sink.h"
#include "ns3/point-to-point-module.h"
#include "ns3/ptr.h"

#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>

using namespace ns3;
using namespace std;

NS_LOG_COMPONENT_DEFINE("ATP-P2P-2w2w-Straggler-Bandwidth");

// 拥塞窗口跟踪，写入txt文件
std::ofstream cwndStream_job1;
std::ofstream cwndStream_job2;

// PacketSink端接收到的job1和job2的字节数
uint64_t lastTimeJob1Bytes = 0;
uint64_t lastTimeJob2Bytes = 0;

std::ofstream sinkBytesStream_job1;
std::ofstream sinkBytesStream_job2;

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

int
main(int argc, char* argv[])
{
    // 日志配置（可根据需要启用）
    // LogComponentEnable("ATPBulkSendApplication", LOG_LEVEL_ALL);
    // LogComponentEnable("PacketSink", LOG_LEVEL_ALL);
    // LogComponentEnable("ATPSocket", LOG_LEVEL_ERROR);
    // LogComponentEnable("ATPL4Protocol", LOG_LEVEL_ALL);
    // LogComponentEnable("PointToPointNetDevice", LOG_LEVEL_INFO);
    // LogComponentEnable("ATPTxBuffer", LOG_LEVEL_ERROR);
    // LogComponentEnable("ATP-P2P-2w2w-Straggler-Bandwidth", LOG_LEVEL_INFO);

    // 配置straggler参数
    uint32_t job1StragglerId = 2;      // Job1的straggler worker ID (0-4) - 固定为w2
    uint32_t job2StragglerId = -7;      // Job2的straggler worker ID (5-9)
    std::string bgDataRate = "70Gbps";

    CommandLine cmd;
    cmd.AddValue("bgDataRate", "Background traffic data rate", bgDataRate);
    cmd.Parse(argc, argv);
    if (bgDataRate == "0Gbps")
    {
        job1StragglerId = -1;
    }

    cwndStream_job1.open("atp-result/trace-atp-newtopo/job1-cwnd-trace-straggler-bw.txt",
                         std::ofstream::out | std::ofstream::trunc);
    cwndStream_job2.open("atp-result/trace-atp-newtopo/job2-cwnd-trace-straggler-bw.txt",
                         std::ofstream::out | std::ofstream::trunc);
    sinkBytesStream_job1.open("atp-result/trace-atp-newtopo/job1-sinkBytes-trace-straggler-bw.txt",
                              std::ofstream::out | std::ofstream::trunc);
    sinkBytesStream_job2.open("atp-result/trace-atp-newtopo/job2-sinkBytes-trace-straggler-bw.txt",
                              std::ofstream::out | std::ofstream::trunc);

    uint32_t maxBytes = 0;
    Time stopTime = Seconds(1.0) + MicroSeconds(20001); // 延长模拟时间以观察straggler效果

    // 设置job1和job2初始拥塞窗口
    uint64_t initialTimestamp = 1000000;
    uint32_t job1_initCwnd = 32;
    uint32_t job2_initCwnd = 32;

    // 注意：超时重传机制已启用
    // 默认超时时间：500微秒，检查间隔：100微秒
    // 可通过下方参数调整
    // 设置超时重传参数
    Time retxTimeout = MicroSeconds(500);      // 重传超时时间：500us
    Time retxCheckInterval = MicroSeconds(100); // 超时检查间隔：100us

    Ipv4Address multicastGroupJob1("225.1.1.1");
    Ipv4Address multicastGroupJob2("225.1.1.2");

    // 在文件打开后，写入初始拥塞窗口值
    cwndStream_job1 << initialTimestamp << "\t" << job1_initCwnd << std::endl;
    cwndStream_job2 << initialTimestamp << "\t" << job2_initCwnd << std::endl;

    // 记录straggler信息
    std::cout << "Straggler Configuration:" << std::endl;
    std::cout << "Job1 Straggler: w" << job1StragglerId << std::endl;
    std::cout << "Job2 Straggler: w" << job2StragglerId << std::endl;
    std::cout << "bgDataRate: " << bgDataRate << std::endl;

    //
    // Explicitly create the nodes required by the topology (shown above).
    //
    NS_LOG_INFO("Create nodes.");
    NodeContainer nodes;
    nodes.Create(18);
    // 节点: w0,w1,w2,w3,w4,w5,w6,w7,w8,w9, s0,s1,s2,s3,ps1,ps2, bg1,bg2
    // job1: w0, w1, w2, w3, w4
    // job2: w5, w6, w7, w8, w9

    NodeContainer w0s0 = NodeContainer(nodes.Get(0), nodes.Get(10));
    NodeContainer w1s0 = NodeContainer(nodes.Get(1), nodes.Get(10));
    NodeContainer w2s0 = NodeContainer(nodes.Get(2), nodes.Get(10));

    NodeContainer w3s1 = NodeContainer(nodes.Get(3), nodes.Get(11));
    NodeContainer w4s1 = NodeContainer(nodes.Get(4), nodes.Get(11));
    NodeContainer w5s1 = NodeContainer(nodes.Get(5), nodes.Get(11));
    NodeContainer w6s1 = NodeContainer(nodes.Get(6), nodes.Get(11));

    NodeContainer w7s2 = NodeContainer(nodes.Get(7), nodes.Get(12));
    NodeContainer w8s2 = NodeContainer(nodes.Get(8), nodes.Get(12));
    NodeContainer w9s2 = NodeContainer(nodes.Get(9), nodes.Get(12));

    NodeContainer s0s3 = NodeContainer(nodes.Get(10), nodes.Get(13));
    NodeContainer s1s3 = NodeContainer(nodes.Get(11), nodes.Get(13));
    NodeContainer s2s3 = NodeContainer(nodes.Get(12), nodes.Get(13));

    NodeContainer s3ps1 = NodeContainer(nodes.Get(13), nodes.Get(14));
    NodeContainer s3ps2 = NodeContainer(nodes.Get(13), nodes.Get(15));

    NodeContainer s0bg1 = NodeContainer(nodes.Get(10), nodes.Get(16)); // 连接s0和bg1
    NodeContainer s2bg2 = NodeContainer(nodes.Get(12), nodes.Get(17)); // 连接s2和bg2

    NS_LOG_INFO("Create channels.");

    //
    // Explicitly create the point-to-point links required by the topology (shown above).
    //
    PointToPointHelper pointToPoint;

    // 正常链路：100Gbps
    pointToPoint.SetDeviceAttribute("DataRate", StringValue("100Gbps"));
    pointToPoint.SetChannelAttribute("Delay", StringValue("1us"));
    pointToPoint.SetQueue("ns3::DropTailQueue", "MaxSize", StringValue("256KB"));

    NetDeviceContainer dev_w0s0, dev_w1s0, dev_w2s0;
    NetDeviceContainer dev_w3s1, dev_w4s1, dev_w5s1, dev_w6s1;
    NetDeviceContainer dev_w7s2, dev_w8s2, dev_w9s2;

    NetDeviceContainer dev_s0s3, dev_s1s3, dev_s2s3;
    NetDeviceContainer dev_s3ps1, dev_s3ps2;

    NetDeviceContainer dev_s0bg1 = pointToPoint.Install(s0bg1);
    NetDeviceContainer dev_s2bg2 = pointToPoint.Install(s2bg2);

    // 安装链路
    NS_LOG_INFO("Configuring straggler links with reduced bandwidth...");

    dev_w0s0 = pointToPoint.Install(w0s0);
    dev_w1s0 = pointToPoint.Install(w1s0);
    dev_w2s0 = pointToPoint.Install(w2s0);
    dev_w3s1 = pointToPoint.Install(w3s1);
    dev_w4s1 = pointToPoint.Install(w4s1);
    dev_w5s1 = pointToPoint.Install(w5s1);
    dev_w6s1 = pointToPoint.Install(w6s1);
    dev_w7s2 = pointToPoint.Install(w7s2);
    dev_w8s2 = pointToPoint.Install(w8s2);
    dev_w9s2 = pointToPoint.Install(w9s2);

    // 交换机之间的链路都是100Gbps
    dev_s0s3 = pointToPoint.Install(s0s3);
    dev_s1s3 = pointToPoint.Install(s1s3);
    dev_s2s3 = pointToPoint.Install(s2s3);
    dev_s3ps1 = pointToPoint.Install(s3ps1);
    dev_s3ps2 = pointToPoint.Install(s3ps2);

    // 设置队列阈值
    Ptr<PointToPointNetDevice> s0s3Device = DynamicCast<PointToPointNetDevice>(dev_s0s3.Get(0));
    Ptr<PointToPointNetDevice> s1s3Device = DynamicCast<PointToPointNetDevice>(dev_s1s3.Get(0));
    Ptr<PointToPointNetDevice> s2s3Device = DynamicCast<PointToPointNetDevice>(dev_s2s3.Get(0));
    Ptr<PointToPointNetDevice> s3ps1Device = DynamicCast<PointToPointNetDevice>(dev_s3ps1.Get(0));
    Ptr<PointToPointNetDevice> s3ps2Device = DynamicCast<PointToPointNetDevice>(dev_s3ps2.Get(0));

    s0s3Device->SetThresholdBytes(50000);
    s1s3Device->SetThresholdBytes(50000);
    s2s3Device->SetThresholdBytes(50000);
    s3ps1Device->SetThresholdBytes(50000);
    s3ps2Device->SetThresholdBytes(50000);

    s0s3Device->SetEnableEcnBytes(true);
    s1s3Device->SetEnableEcnBytes(true);
    s2s3Device->SetEnableEcnBytes(true);
    s3ps1Device->SetEnableEcnBytes(true);
    s3ps2Device->SetEnableEcnBytes(true);

    //
    // Install the internet stack on the nodes
    //
    InternetStackHelper internet;
    internet.SetRoutingHelper(ATPStaticRoutingHelper());
    internet.Install(nodes);

    //
    // We've got the "hardware" in place.  Now we need to add IP addresses.
    //
    NS_LOG_INFO("Assign IP Addresses.");
    Ipv4AddressHelper ipv4Helper;

    ipv4Helper.SetBase("10.1.1.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w0s0 = ipv4Helper.Assign(dev_w0s0);

    ipv4Helper.SetBase("10.1.2.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w1s0 = ipv4Helper.Assign(dev_w1s0);

    ipv4Helper.SetBase("10.1.3.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w2s0 = ipv4Helper.Assign(dev_w2s0);

    ipv4Helper.SetBase("10.1.4.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w3s1 = ipv4Helper.Assign(dev_w3s1);

    ipv4Helper.SetBase("10.1.5.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w4s1 = ipv4Helper.Assign(dev_w4s1);

    ipv4Helper.SetBase("10.1.6.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w5s1 = ipv4Helper.Assign(dev_w5s1);

    ipv4Helper.SetBase("10.1.7.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w6s1 = ipv4Helper.Assign(dev_w6s1);

    ipv4Helper.SetBase("10.1.8.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w7s2 = ipv4Helper.Assign(dev_w7s2);

    ipv4Helper.SetBase("10.1.9.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w8s2 = ipv4Helper.Assign(dev_w8s2);

    ipv4Helper.SetBase("10.1.10.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_w9s2 = ipv4Helper.Assign(dev_w9s2);

    ipv4Helper.SetBase("10.1.11.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s0s3 = ipv4Helper.Assign(dev_s0s3);

    ipv4Helper.SetBase("10.1.12.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s1s3 = ipv4Helper.Assign(dev_s1s3);

    ipv4Helper.SetBase("10.1.13.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s2s3 = ipv4Helper.Assign(dev_s2s3);

    ipv4Helper.SetBase("10.1.14.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s3ps1 = ipv4Helper.Assign(dev_s3ps1);

    ipv4Helper.SetBase("10.1.15.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s3ps2 = ipv4Helper.Assign(dev_s3ps2);

    ipv4Helper.SetBase("10.1.16.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s0bg1 = ipv4Helper.Assign(dev_s0bg1);

    ipv4Helper.SetBase("10.1.17.0", "255.255.255.0");
    Ipv4InterfaceContainer ip_s2bg2 = ipv4Helper.Assign(dev_s2bg2);

    //
    // Create a PacketSinkApplication and install it on node 3
    //
    NS_LOG_INFO("Create Applications.");

    // Create sink applications for both jobs
    Ptr<ATPPacketSink> sinkApp1 = CreateObject<ATPPacketSink>();
    Ptr<ATPPacketSink> sinkApp2 = CreateObject<ATPPacketSink>();

    // job1 sink (ps1)
    uint16_t sinkPort1 = 9;
    Address sinkAddress1(InetSocketAddress(ip_s3ps1.GetAddress(1), sinkPort1));
    sinkApp1->SetAddressPort(sinkAddress1, sinkPort1);

    // job2 sink (ps2)
    uint16_t sinkPort2 = 9;
    Address sinkAddress2(InetSocketAddress(ip_s3ps2.GetAddress(1), sinkPort2));
    sinkApp2->SetAddressPort(sinkAddress2, sinkPort2);

    // create ATPSocket for sink1 and bind to sinkAddress1
    Ptr<Socket> sinkSocket1 = Socket::CreateSocket(nodes.Get(14), ATPSocketFactory::GetTypeId());
    Ptr<ATPSocket> sinkATPSocket1 = DynamicCast<ATPSocket>(sinkSocket1);
    sinkApp1->SetSocket(sinkATPSocket1);
    sinkATPSocket1->Bind(sinkAddress1);
    sinkATPSocket1->Listen();

    // create ATPSocket for sink2 and bind to sinkAddress2
    Ptr<Socket> sinkSocket2 = Socket::CreateSocket(nodes.Get(15), ATPSocketFactory::GetTypeId());
    Ptr<ATPSocket> sinkATPSocket2 = DynamicCast<ATPSocket>(sinkSocket2);
    sinkApp2->SetSocket(sinkATPSocket2);
    sinkATPSocket2->Bind(sinkAddress2);
    sinkATPSocket2->Listen();

    // start sink applications
    sinkApp1->SetStartTime(Seconds(0.0));
    sinkApp1->SetStopTime(stopTime);
    nodes.Get(14)->AddApplication(sinkApp1); // ps1

    sinkApp2->SetStartTime(Seconds(0.0));
    sinkApp2->SetStopTime(stopTime);
    nodes.Get(15)->AddApplication(sinkApp2); // ps2

    //
    // Create sockets for job1 (w0, w1, w2, w3, w4)
    //
    Ptr<Socket> w0job1socket = Socket::CreateSocket(nodes.Get(0), ATPSocketFactory::GetTypeId());
    Ptr<Socket> w1job1socket = Socket::CreateSocket(nodes.Get(1), ATPSocketFactory::GetTypeId());
    Ptr<Socket> w2job1socket = Socket::CreateSocket(nodes.Get(2), ATPSocketFactory::GetTypeId());
    Ptr<Socket> w3job1socket = Socket::CreateSocket(nodes.Get(3), ATPSocketFactory::GetTypeId());
    Ptr<Socket> w4job1socket = Socket::CreateSocket(nodes.Get(4), ATPSocketFactory::GetTypeId());

    Ptr<ATPSocket> w0job1_ATPSocket = DynamicCast<ATPSocket>(w0job1socket);
    Ptr<ATPSocket> w1job1_ATPSocket = DynamicCast<ATPSocket>(w1job1socket);
    Ptr<ATPSocket> w2job1_ATPSocket = DynamicCast<ATPSocket>(w2job1socket);
    Ptr<ATPSocket> w3job1_ATPSocket = DynamicCast<ATPSocket>(w3job1socket);
    Ptr<ATPSocket> w4job1_ATPSocket = DynamicCast<ATPSocket>(w4job1socket);
    //
    // Create sockets for job2 (w5, w6, w7, w8, w9)
    //
    Ptr<Socket> w5job2socket = Socket::CreateSocket(nodes.Get(5), ATPSocketFactory::GetTypeId());
    Ptr<Socket> w6job2socket = Socket::CreateSocket(nodes.Get(6), ATPSocketFactory::GetTypeId());
    Ptr<Socket> w7job2socket = Socket::CreateSocket(nodes.Get(7), ATPSocketFactory::GetTypeId());
    Ptr<Socket> w8job2socket = Socket::CreateSocket(nodes.Get(8), ATPSocketFactory::GetTypeId());
    Ptr<Socket> w9job2socket = Socket::CreateSocket(nodes.Get(9), ATPSocketFactory::GetTypeId());

    Ptr<ATPSocket> w5job2_ATPSocket = DynamicCast<ATPSocket>(w5job2socket);
    Ptr<ATPSocket> w6job2_ATPSocket = DynamicCast<ATPSocket>(w6job2socket);
    Ptr<ATPSocket> w7job2_ATPSocket = DynamicCast<ATPSocket>(w7job2socket);
    Ptr<ATPSocket> w8job2_ATPSocket = DynamicCast<ATPSocket>(w8job2socket);
    Ptr<ATPSocket> w9job2_ATPSocket = DynamicCast<ATPSocket>(w9job2socket);

    // Create applications for job1
    Ptr<ATPBulkSendApplication> w0job1App = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> w1job1App = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> w2job1App = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> w3job1App = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> w4job1App = CreateObject<ATPBulkSendApplication>();

    // Create applications for job2
    Ptr<ATPBulkSendApplication> w5job2App = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> w6job2App = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> w7job2App = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> w8job2App = CreateObject<ATPBulkSendApplication>();
    Ptr<ATPBulkSendApplication> w9job2App = CreateObject<ATPBulkSendApplication>();

    uint16_t sendPort1 = 11; // Port for job1
    uint16_t sendPort2 = 12; // Port for job2

    // Addresses for job1 workers
    Address anyAddressJob1(InetSocketAddress(Ipv4Address::GetAny(), sendPort1));

    // Addresses for job2 workers
    Address anyAddressJob2(InetSocketAddress(Ipv4Address::GetAny(), sendPort2));

    // 设置job1初始拥塞窗口和超时参数
    w0job1_ATPSocket->SetInitCwnd(job1_initCwnd);
    w0job1_ATPSocket->SetRetxTimeout(retxTimeout);
    w0job1_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w0job1_ATPSocket->SetNumWorkers(5);

    w1job1_ATPSocket->SetInitCwnd(job1_initCwnd);
    w1job1_ATPSocket->SetRetxTimeout(retxTimeout);
    w1job1_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w1job1_ATPSocket->SetNumWorkers(5);

    w2job1_ATPSocket->SetInitCwnd(job1_initCwnd);
    w2job1_ATPSocket->SetRetxTimeout(retxTimeout);
    w2job1_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w2job1_ATPSocket->SetNumWorkers(5);

    w3job1_ATPSocket->SetInitCwnd(job1_initCwnd);
    w3job1_ATPSocket->SetRetxTimeout(retxTimeout);
    w3job1_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w3job1_ATPSocket->SetNumWorkers(5);

    w4job1_ATPSocket->SetInitCwnd(job1_initCwnd);
    w4job1_ATPSocket->SetRetxTimeout(retxTimeout);
    w4job1_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w4job1_ATPSocket->SetNumWorkers(5);

    // 设置job2初始拥塞窗口和超时参数
    w5job2_ATPSocket->SetInitCwnd(job2_initCwnd);
    w5job2_ATPSocket->SetRetxTimeout(retxTimeout);
    w5job2_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w5job2_ATPSocket->SetNumWorkers(5);

    w6job2_ATPSocket->SetInitCwnd(job2_initCwnd);
    w6job2_ATPSocket->SetRetxTimeout(retxTimeout);
    w6job2_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w6job2_ATPSocket->SetNumWorkers(5);

    w7job2_ATPSocket->SetInitCwnd(job2_initCwnd);
    w7job2_ATPSocket->SetRetxTimeout(retxTimeout);
    w7job2_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w7job2_ATPSocket->SetNumWorkers(5);

    w8job2_ATPSocket->SetInitCwnd(job2_initCwnd);
    w8job2_ATPSocket->SetRetxTimeout(retxTimeout);
    w8job2_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w8job2_ATPSocket->SetNumWorkers(5);

    w9job2_ATPSocket->SetInitCwnd(job2_initCwnd);
    w9job2_ATPSocket->SetRetxTimeout(retxTimeout);
    w9job2_ATPSocket->SetRetxCheckInterval(retxCheckInterval);
    w9job2_ATPSocket->SetNumWorkers(5);

    // Configure w0job1App
    uint8_t job1Id = 1;
    uint8_t job1_s0_faninDegree0 = 3; // w0, w1, w2
    uint8_t job1_s1_faninDegree0 = 2; // w3, w4
    uint8_t job1_s3_faninDegree1 = 2; // s0, s1

    w0job1App->Setup(sinkAddress1, w0job1_ATPSocket, maxBytes, job1Id);
    w0job1App->SetEnableATPTag(true);
    w0job1App->SetStartTime(Seconds(1.0));
    w0job1App->SetStopTime(stopTime);
    w0job1App->SetFaninDegree0(job1_s0_faninDegree0);
    w0job1App->SetFaninDegree1(job1_s3_faninDegree1);
    w0job1App->SetBitmap0(0b001);
    w0job1App->SetBitmap1(0b01);

    w0job1_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w0job1App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w0job1App));
    w0job1_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w0job1App));
    w0job1_ATPSocket->Bind(anyAddressJob1);
    w0job1_ATPSocket->Connect(sinkAddress1);
    nodes.Get(0)->AddApplication(w0job1App);

    // Configure w1job1App
    w1job1App->Setup(sinkAddress1, w1job1_ATPSocket, maxBytes, job1Id);
    w1job1App->SetEnableATPTag(true);
    w1job1App->SetStartTime(Seconds(1.0));
    w1job1App->SetStopTime(stopTime);
    w1job1App->SetFaninDegree0(job1_s0_faninDegree0);
    w1job1App->SetFaninDegree1(job1_s3_faninDegree1);
    w1job1App->SetBitmap0(0b010);
    w1job1App->SetBitmap1(0b01);

    w1job1_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w1job1App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w1job1App));
    w1job1_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w1job1App));
    w1job1_ATPSocket->Bind(anyAddressJob1);
    w1job1_ATPSocket->Connect(sinkAddress1);
    nodes.Get(1)->AddApplication(w1job1App);

    // Configure w2job1App
    w2job1App->Setup(sinkAddress1, w2job1_ATPSocket, maxBytes, job1Id);
    w2job1App->SetEnableATPTag(true);
    w2job1App->SetStartTime(Seconds(1.0));
    w2job1App->SetStopTime(stopTime);
    w2job1App->SetFaninDegree0(job1_s0_faninDegree0);
    w2job1App->SetFaninDegree1(job1_s3_faninDegree1);
    w2job1App->SetBitmap0(0b100);
    w2job1App->SetBitmap1(0b01);

    w2job1_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w2job1App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w2job1App));
    w2job1_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w2job1App));
    w2job1_ATPSocket->Bind(anyAddressJob1);
    w2job1_ATPSocket->Connect(sinkAddress1);
    nodes.Get(2)->AddApplication(w2job1App);

    // Configure w3job1App
    w3job1App->Setup(sinkAddress1, w3job1_ATPSocket, maxBytes, job1Id);
    w3job1App->SetEnableATPTag(true);
    w3job1App->SetStartTime(Seconds(1.0));
    w3job1App->SetStopTime(stopTime);
    w3job1App->SetFaninDegree0(job1_s1_faninDegree0);
    w3job1App->SetFaninDegree1(job1_s3_faninDegree1);
    w3job1App->SetBitmap0(0b01);
    w3job1App->SetBitmap1(0b10);

    w3job1_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w3job1App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w3job1App));
    w3job1_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w3job1App));
    w3job1_ATPSocket->Bind(anyAddressJob1);
    w3job1_ATPSocket->Connect(sinkAddress1);
    nodes.Get(3)->AddApplication(w3job1App);

    // Configure w4job1App
    w4job1App->Setup(sinkAddress1, w4job1_ATPSocket, maxBytes, job1Id);
    w4job1App->SetEnableATPTag(true);
    w4job1App->SetStartTime(Seconds(1.0));
    w4job1App->SetStopTime(stopTime);
    w4job1App->SetFaninDegree0(job1_s1_faninDegree0);
    w4job1App->SetFaninDegree1(job1_s3_faninDegree1);
    w4job1App->SetBitmap0(0b10);
    w4job1App->SetBitmap1(0b10);

    w4job1_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w4job1App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w4job1App));
    w4job1_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w4job1App));
    w4job1_ATPSocket->Bind(anyAddressJob1);
    w4job1_ATPSocket->Connect(sinkAddress1);
    nodes.Get(4)->AddApplication(w4job1App);

    // Configure w5job2App
    uint8_t job2Id = 2;
    uint8_t job2_s1_faninDegree0 = 2; // w5, w6
    uint8_t job2_s2_faninDegree0 = 3; // w7, w8, w9
    uint8_t job2_s3_faninDegree1 = 2; // s1, s2
    w5job2App->Setup(sinkAddress2, w5job2_ATPSocket, maxBytes, job2Id);
    w5job2App->SetEnableATPTag(true);
    w5job2App->SetStartTime(Seconds(1.0));
    w5job2App->SetStopTime(stopTime);
    w5job2App->SetFaninDegree0(job2_s1_faninDegree0);
    w5job2App->SetFaninDegree1(job2_s3_faninDegree1);
    w5job2App->SetBitmap0(0b01);
    w5job2App->SetBitmap1(0b01);

    w5job2_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w5job2App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w5job2App));
    w5job2_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w5job2App));
    w5job2_ATPSocket->Bind(anyAddressJob2);
    w5job2_ATPSocket->Connect(sinkAddress2);
    nodes.Get(5)->AddApplication(w5job2App);

    // Configure w6job2App
    w6job2App->Setup(sinkAddress2, w6job2_ATPSocket, maxBytes, job2Id);
    w6job2App->SetEnableATPTag(true);
    w6job2App->SetStartTime(Seconds(1.0));
    w6job2App->SetStopTime(stopTime);
    w6job2App->SetFaninDegree0(job2_s1_faninDegree0);
    w6job2App->SetFaninDegree1(job2_s3_faninDegree1);
    w6job2App->SetBitmap0(0b10);
    w6job2App->SetBitmap1(0b01);

    w6job2_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w6job2App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w6job2App));
    w6job2_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w6job2App));
    w6job2_ATPSocket->Bind(anyAddressJob2);
    w6job2_ATPSocket->Connect(sinkAddress2);
    nodes.Get(6)->AddApplication(w6job2App);

    // Configure w7job2App
    w7job2App->Setup(sinkAddress2, w7job2_ATPSocket, maxBytes, job2Id);
    w7job2App->SetEnableATPTag(true);
    w7job2App->SetStartTime(Seconds(1.0));
    w7job2App->SetStopTime(stopTime);
    w7job2App->SetFaninDegree0(job2_s2_faninDegree0);
    w7job2App->SetFaninDegree1(job2_s3_faninDegree1);
    w7job2App->SetBitmap0(0b001);
    w7job2App->SetBitmap1(0b10);

    w7job2_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w7job2App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w7job2App));
    w7job2_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w7job2App));
    w7job2_ATPSocket->Bind(anyAddressJob2);
    w7job2_ATPSocket->Connect(sinkAddress2);
    nodes.Get(7)->AddApplication(w7job2App);

    // Configure w8job2App
    w8job2App->Setup(sinkAddress2, w8job2_ATPSocket, maxBytes, job2Id);
    w8job2App->SetEnableATPTag(true);
    w8job2App->SetStartTime(Seconds(1.0));
    w8job2App->SetStopTime(stopTime);
    w8job2App->SetFaninDegree0(job2_s2_faninDegree0);
    w8job2App->SetFaninDegree1(job2_s3_faninDegree1);
    w8job2App->SetBitmap0(0b010);
    w8job2App->SetBitmap1(0b10);

    w8job2_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w8job2App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w8job2App));
    w8job2_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w8job2App));
    w8job2_ATPSocket->Bind(anyAddressJob2);
    w8job2_ATPSocket->Connect(sinkAddress2);
    nodes.Get(8)->AddApplication(w8job2App);

    // Configure w9job2App
    w9job2App->Setup(sinkAddress2, w9job2_ATPSocket, maxBytes, job2Id);
    w9job2App->SetEnableATPTag(true);
    w9job2App->SetStartTime(Seconds(1.0));
    w9job2App->SetStopTime(stopTime);
    w9job2App->SetFaninDegree0(job2_s2_faninDegree0);
    w9job2App->SetFaninDegree1(job2_s3_faninDegree1);
    w9job2App->SetBitmap0(0b100);
    w9job2App->SetBitmap1(0b10);

    w9job2_ATPSocket->SetConnectCallback(
        MakeCallback(&ATPBulkSendApplication::ConnectionSucceeded, w9job2App),
        MakeCallback(&ATPBulkSendApplication::ConnectionFailed, w9job2App));
    w9job2_ATPSocket->SetSendCallback(MakeCallback(&ATPBulkSendApplication::DataSend, w9job2App));
    w9job2_ATPSocket->Bind(anyAddressJob2);
    w9job2_ATPSocket->Connect(sinkAddress2);
    nodes.Get(9)->AddApplication(w9job2App);

    // 连接拥塞窗口跟踪
    w0job1_ATPSocket->GetTxBuffer()->TraceConnectWithoutContext("cwndTrace",
                                                                MakeCallback(&CwndChange_job1));
    w9job2_ATPSocket->GetTxBuffer()->TraceConnectWithoutContext("cwndTrace",
                                                                MakeCallback(&CwndChange_job2));

    // 获取每个节点的静态路由对象
    ATPStaticRoutingHelper staticRoutingHelper;
    Ptr<ATPStaticRouting> staticRouting_w0 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(0)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_w1 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(1)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_w2 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(2)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_w3 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(3)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_w4 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(4)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_w5 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(5)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_w6 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(6)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_w7 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(7)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_w8 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(8)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_w9 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(9)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_s0 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(10)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_s1 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(11)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_s2 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(12)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_s3 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(13)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_ps1 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(14)->GetObject<Ipv4>());
    Ptr<ATPStaticRouting> staticRouting_ps2 =
        staticRoutingHelper.GetStaticRouting(nodes.Get(15)->GetObject<Ipv4>());

    staticRouting_s0->SetEnableAggregation(true);
    staticRouting_s1->SetEnableAggregation(true);
    staticRouting_s2->SetEnableAggregation(true);
    staticRouting_s3->SetEnableAggregation(true);
    staticRouting_s0->GetATPL4Protocol()->SetEnableHierarchicalAggregation(true);
    staticRouting_s1->GetATPL4Protocol()->SetEnableHierarchicalAggregation(true);
    staticRouting_s2->GetATPL4Protocol()->SetEnableHierarchicalAggregation(true);
    staticRouting_s3->GetATPL4Protocol()->SetEnableHierarchicalAggregation(true);

    // 配置w0的路由表 w0->s0->s3->ps1
    staticRouting_w0->AddHostRouteTo(ip_s3ps1.GetAddress(1), ip_w0s0.GetAddress(1), 1);

    // 配置w1的路由表 w1->s0->s3->ps1
    staticRouting_w1->AddHostRouteTo(ip_s3ps1.GetAddress(1), ip_w1s0.GetAddress(1), 1);

    // 配置w2的路由表 w2->s0->s3->ps1
    staticRouting_w2->AddHostRouteTo(ip_s3ps1.GetAddress(1), ip_w2s0.GetAddress(1), 1);

    // 配置w3的路由表 w3->s1->s3->ps1
    staticRouting_w3->AddHostRouteTo(ip_s3ps1.GetAddress(1), ip_w3s1.GetAddress(1), 1);

    // 配置w4的路由表 w4->s1->s3->ps1
    staticRouting_w4->AddHostRouteTo(ip_s3ps1.GetAddress(1), ip_w4s1.GetAddress(1), 1);

    // 配置w5的路由表 w5->s1->s3->ps2
    staticRouting_w5->AddHostRouteTo(ip_s3ps2.GetAddress(1), ip_w5s1.GetAddress(1), 1);

    // 配置w6的路由表 w6->s1->s3->ps2
    staticRouting_w6->AddHostRouteTo(ip_s3ps2.GetAddress(1), ip_w6s1.GetAddress(1), 1);

    // 配置w7的路由表 w7->s2->s3->ps2
    staticRouting_w7->AddHostRouteTo(ip_s3ps2.GetAddress(1), ip_w7s2.GetAddress(1), 1);

    // 配置w8的路由表 w8->s2->s3->ps2
    staticRouting_w8->AddHostRouteTo(ip_s3ps2.GetAddress(1), ip_w8s2.GetAddress(1), 1);

    // 配置w9的路由表 w9->s2->s3->ps2
    staticRouting_w9->AddHostRouteTo(ip_s3ps2.GetAddress(1), ip_w9s2.GetAddress(1), 1);

    // 配置s0的路由表
    // s0->w0, w1, w2
    staticRouting_s0->AddMulticastRoute(ip_s3ps1.GetAddress(1), multicastGroupJob1, 4, {1, 2, 3});
    // s0->w0
    staticRouting_s0->AddHostRouteTo(ip_w0s0.GetAddress(0), ip_w0s0.GetAddress(0), 1);
    // s0->w1
    staticRouting_s0->AddHostRouteTo(ip_w1s0.GetAddress(0), ip_w1s0.GetAddress(0), 2);
    // s0->w2
    staticRouting_s0->AddHostRouteTo(ip_w2s0.GetAddress(0), ip_w2s0.GetAddress(0), 3);
    // s0->s3->ps1
    staticRouting_s0->AddHostRouteTo(ip_s3ps1.GetAddress(1), ip_s0s3.GetAddress(1), 4);
    // s0->bg1
    staticRouting_s0->AddHostRouteTo(ip_s0bg1.GetAddress(1), ip_s0bg1.GetAddress(1), 5);

    // 配置s1的路由表
    // s1->w3, w4
    staticRouting_s1->AddMulticastRoute(ip_s3ps1.GetAddress(1), multicastGroupJob1, 5, {1, 2});
    // s1->w5, w6
    staticRouting_s1->AddMulticastRoute(ip_s3ps2.GetAddress(1), multicastGroupJob2, 5, {3, 4});
    // s1->w3
    staticRouting_s1->AddHostRouteTo(ip_w3s1.GetAddress(0), ip_w3s1.GetAddress(0), 1);
    // s1->w4
    staticRouting_s1->AddHostRouteTo(ip_w4s1.GetAddress(0), ip_w4s1.GetAddress(0), 2);
    // s1->w5
    staticRouting_s1->AddHostRouteTo(ip_w5s1.GetAddress(0), ip_w5s1.GetAddress(0), 3);
    // s1->w6
    staticRouting_s1->AddHostRouteTo(ip_w6s1.GetAddress(0), ip_w6s1.GetAddress(0), 4);
    // s1->s3->ps1
    staticRouting_s1->AddHostRouteTo(ip_s3ps1.GetAddress(1), ip_s1s3.GetAddress(1), 5);
    // s1->s3->ps2
    staticRouting_s1->AddHostRouteTo(ip_s3ps2.GetAddress(1), ip_s1s3.GetAddress(1), 5);

    // 配置s2的路由表
    // s2->w7, w8, w9
    staticRouting_s2->AddMulticastRoute(ip_s3ps2.GetAddress(1), multicastGroupJob2, 4, {1, 2, 3});
    // s2->w7
    staticRouting_s2->AddHostRouteTo(ip_w7s2.GetAddress(0), ip_w7s2.GetAddress(0), 1);
    // s2->w8
    staticRouting_s2->AddHostRouteTo(ip_w8s2.GetAddress(0), ip_w8s2.GetAddress(0), 2);
    // s2->w9
    staticRouting_s2->AddHostRouteTo(ip_w9s2.GetAddress(0), ip_w9s2.GetAddress(0), 3);
    // s2->s3->ps2
    staticRouting_s2->AddHostRouteTo(ip_s3ps2.GetAddress(1), ip_s2s3.GetAddress(1), 4);
    // s2->bg2
    staticRouting_s2->AddHostRouteTo(ip_s2bg2.GetAddress(1), ip_s2bg2.GetAddress(1), 5);

    // 配置s3的路由表
    // s3->s0, s1
    staticRouting_s3->AddMulticastRoute(ip_s3ps1.GetAddress(1), multicastGroupJob1, 4, {1, 2});
    // s3->s1, s2
    staticRouting_s3->AddMulticastRoute(ip_s3ps2.GetAddress(1), multicastGroupJob2, 5, {2, 3});
    // s3->s0->w0
    staticRouting_s3->AddHostRouteTo(ip_w0s0.GetAddress(0), ip_s0s3.GetAddress(0), 1);
    // s3->s0->w1
    staticRouting_s3->AddHostRouteTo(ip_w1s0.GetAddress(0), ip_s0s3.GetAddress(0), 1);
    // s3->s0->w2
    staticRouting_s3->AddHostRouteTo(ip_w2s0.GetAddress(0), ip_s0s3.GetAddress(0), 1);
    // s3->s1->w3
    staticRouting_s3->AddHostRouteTo(ip_w3s1.GetAddress(0), ip_s1s3.GetAddress(0), 2);
    // s3->s1->w4
    staticRouting_s3->AddHostRouteTo(ip_w4s1.GetAddress(0), ip_s1s3.GetAddress(0), 2);
    // s3->s1->w5
    staticRouting_s3->AddHostRouteTo(ip_w5s1.GetAddress(0), ip_s1s3.GetAddress(0), 2);
    // s3->s1->w6
    staticRouting_s3->AddHostRouteTo(ip_w6s1.GetAddress(0), ip_s1s3.GetAddress(0), 2);
    // s3->s2->w7
    staticRouting_s3->AddHostRouteTo(ip_w7s2.GetAddress(0), ip_s2s3.GetAddress(0), 3);
    // s3->s2->w8
    staticRouting_s3->AddHostRouteTo(ip_w8s2.GetAddress(0), ip_s2s3.GetAddress(0), 3);
    // s3->s2->w9
    staticRouting_s3->AddHostRouteTo(ip_w9s2.GetAddress(0), ip_s2s3.GetAddress(0), 3);
    // s3->ps1
    staticRouting_s3->AddHostRouteTo(ip_s3ps1.GetAddress(1), ip_s3ps1.GetAddress(1), 4);
    // s3->ps2
    staticRouting_s3->AddHostRouteTo(ip_s3ps2.GetAddress(1), ip_s3ps2.GetAddress(1), 5);

    // 配置ps1的路由表
    // ps1->s3
    staticRouting_ps1->SetDefaultMulticastRoute(1);
    //  ps1->s3->s0->w0
    staticRouting_ps1->AddHostRouteTo(ip_w0s0.GetAddress(0), ip_s3ps1.GetAddress(0), 1);
    // ps1->s3->s0->w1
    staticRouting_ps1->AddHostRouteTo(ip_w1s0.GetAddress(0), ip_s3ps1.GetAddress(0), 1);
    // ps1->s3->s0->w2
    staticRouting_ps1->AddHostRouteTo(ip_w2s0.GetAddress(0), ip_s3ps1.GetAddress(0), 1);
    // ps1->s3->s1->w3
    staticRouting_ps1->AddHostRouteTo(ip_w3s1.GetAddress(0), ip_s3ps1.GetAddress(0), 1);
    // ps1->s3->s1->w4
    staticRouting_ps1->AddHostRouteTo(ip_w4s1.GetAddress(0), ip_s3ps1.GetAddress(0), 1);

    // 配置ps2的路由表
    // ps1->s3
    staticRouting_ps2->SetDefaultMulticastRoute(1);
    //  ps2->s3->s1->w5
    staticRouting_ps2->AddHostRouteTo(ip_w5s1.GetAddress(0), ip_s3ps2.GetAddress(0), 1);
    // ps2->s3->s1->w6
    staticRouting_ps2->AddHostRouteTo(ip_w6s1.GetAddress(0), ip_s3ps2.GetAddress(0), 1);
    // ps2->s3->s2->w7
    staticRouting_ps2->AddHostRouteTo(ip_w7s2.GetAddress(0), ip_s3ps2.GetAddress(0), 1);
    // ps2->s3->s2->w8
    staticRouting_ps2->AddHostRouteTo(ip_w8s2.GetAddress(0), ip_s3ps2.GetAddress(0), 1);
    // ps2->s3->s2->w9
    staticRouting_ps2->AddHostRouteTo(ip_w9s2.GetAddress(0), ip_s3ps2.GetAddress(0), 1);

    // 添加地址映射
    sinkATPSocket1->AddAddressMapping(job1Id, multicastGroupJob1, sendPort1); // job1
    sinkATPSocket2->AddAddressMapping(job2Id, multicastGroupJob2, sendPort2); // job2

    // 配置Job1的树形拓扑结构
    JobTree job1Tree(job1Id);
    job1Tree.fullBitmap1 = 0b11;                        // 2 (s0 和 s1)
    job1Tree.fanInDegree1 = job1_s3_faninDegree1;       // 2 (s0 和 s1)
    job1Tree.AddBranch(0, job1_s0_faninDegree0, 0b111); // bitmap1位0: s0有3个workers
    job1Tree.AddBranch(1, job1_s1_faninDegree0, 0b11);  // bitmap1位1: s1有2个workers
    job1Tree.expectedFlatBitmap = 0b11111;              // 5个workers
    sinkATPSocket1->SetJobTree(job1Tree);

    // 配置Job2的树形拓扑结构
    JobTree job2Tree(job2Id);
    job2Tree.fullBitmap1 = 0b11;                        // 2 (s1 和 s2)
    job2Tree.fanInDegree1 = job2_s3_faninDegree1;       // 2 (s1 和 s2)
    job2Tree.AddBranch(0, job2_s1_faninDegree0, 0b11);  // bitmap1位0: s1有2个workers
    job2Tree.AddBranch(1, job2_s2_faninDegree0, 0b111); // bitmap1位1: s2有3个workers
    job2Tree.expectedFlatBitmap = 0b11111;              // 5个workers
    sinkATPSocket2->SetJobTree(job2Tree);

    NS_LOG_INFO("Configure Background Traffic for Stragglers...");

    uint16_t bgPort = 5000; // 背景流量专用端口

    // 1. 配置 Sink 接收背景流量
    PacketSinkHelper bgSinkHelper("ns3::UdpSocketFactory",
                                  InetSocketAddress(Ipv4Address::GetAny(), bgPort));
    ApplicationContainer bgSinks;
    bgSinks.Start(Seconds(0.0));
    bgSinks.Stop(stopTime);

    // 2. 配置 OnOffApplication 生成恒定码率的流量
    OnOffHelper bgSourceHelper("ns3::UdpSocketFactory", Address());
    // 持续开启，关闭时间设为0
    bgSourceHelper.SetAttribute("OnTime", StringValue("ns3::ConstantRandomVariable[Constant=1]"));
    bgSourceHelper.SetAttribute("OffTime", StringValue("ns3::ConstantRandomVariable[Constant=0]"));

    // 设置占据的带宽。100Gbps的网卡，如果你想模拟极度拥挤，可以设为 70Gbps 或 80Gbps
    bgSourceHelper.SetAttribute("DataRate", StringValue(bgDataRate));
    bgSourceHelper.SetAttribute("PacketSize", UintegerValue(1500)); // 1 MTU

    ApplicationContainer bgSources;

    // 为 Job1 的 Straggler (w2, node 2) 添加背景流量
    if (job1StragglerId == 2)
    {
        // 1. 在 bg1 上安装 Sink
        bgSinks.Add(bgSinkHelper.Install(nodes.Get(16)));

        // 2. 为 w2 添加去往 bg1 的特定路由
        staticRouting_w2->AddHostRouteTo(ip_s0bg1.GetAddress(1), ip_w2s0.GetAddress(1), 1);

        // 3. 在 w2 (Node 2) 上安装 Source，发往 bg1
        Address bgDest1(InetSocketAddress(ip_s0bg1.GetAddress(1), bgPort));
        bgSourceHelper.SetAttribute("Remote", AddressValue(bgDest1));
        bgSources.Add(bgSourceHelper.Install(nodes.Get(2)));

        NS_LOG_INFO("Added " << bgDataRate << " background traffic on w2 -> bg1");
    }

    // 如果你有给 Job2 设置 Straggler，同理添加（这里以 w7, node 7 为例）
    if (job2StragglerId == 7)
    {
        bgSinks.Add(bgSinkHelper.Install(nodes.Get(17))); // bg2
        staticRouting_w7->AddHostRouteTo(ip_s2bg2.GetAddress(1), ip_w7s2.GetAddress(1), 1);

        Address bgDest2(InetSocketAddress(ip_s2bg2.GetAddress(1), bgPort));
        bgSourceHelper.SetAttribute("Remote", AddressValue(bgDest2));
        bgSources.Add(bgSourceHelper.Install(nodes.Get(7)));

        NS_LOG_INFO("Added " << bgDataRate << " background traffic on w7 -> bg2");
    }

    // 背景流量在 0.5 秒启动，提前铺满队列，ATP 流量 1.0 秒启动时就会感受到真实的拥塞
    bgSources.Start(Seconds(0.5));
    bgSources.Stop(stopTime);

    // 开始测量 (目标改为 sinkApp1 和 sinkApp2)
    Simulator::Schedule(Seconds(1.0), &MeasurementRxJob1, sinkApp1);
    Simulator::Schedule(Seconds(1.0), &MeasurementRxJob2, sinkApp2);

    //
    // Now, do the actual simulation.
    //
    NS_LOG_INFO("Run Simulation.");
    Simulator::Stop(stopTime);
    Simulator::Run();
    Simulator::Destroy();
    NS_LOG_INFO("Done.");

    cwndStream_job1.close();
    cwndStream_job2.close();
    sinkBytesStream_job1.close();
    sinkBytesStream_job2.close();

    // 输出详细统计信息
    std::cout << "\n=== 模拟结果总结 (Straggler带宽限制测试) ===" << std::endl;

    std::cout << "Job1 (ID: " << static_cast<int>(job1Id) << "):" << std::endl;
    std::cout << "  w0 Total Bytes Sent: " << w0job1_ATPSocket->GetTotalTxBytes() << std::endl;
    std::cout << "  w1 Total Bytes Sent: " << w1job1_ATPSocket->GetTotalTxBytes() << std::endl;
    std::cout << "  w2 Total Bytes Sent: " << w2job1_ATPSocket->GetTotalTxBytes() << std::endl;
    std::cout << "  w3 Total Bytes Sent: " << w3job1_ATPSocket->GetTotalTxBytes() << std::endl;
    std::cout << "  w4 Total Bytes Sent: " << w4job1_ATPSocket->GetTotalTxBytes() << std::endl;

    uint64_t job1TotalSent =
        w0job1_ATPSocket->GetTotalTxBytes() + w1job1_ATPSocket->GetTotalTxBytes() +
        w2job1_ATPSocket->GetTotalTxBytes() + w3job1_ATPSocket->GetTotalTxBytes() +
        w4job1_ATPSocket->GetTotalTxBytes();
    std::cout << "  Total Sent: " << job1TotalSent << std::endl;
    std::cout << "  Total Received: " << sinkApp1->GetTotalRxJob(job1Id) << std::endl;

    std::cout << "Job2 (ID: " << static_cast<int>(job2Id) << "):" << std::endl;
    std::cout << "  w5 Total Bytes Sent: " << w5job2_ATPSocket->GetTotalTxBytes() << std::endl;
    std::cout << "  w6 Total Bytes Sent: " << w6job2_ATPSocket->GetTotalTxBytes() << std::endl;
    std::cout << "  w7 Total Bytes Sent: " << w7job2_ATPSocket->GetTotalTxBytes() << std::endl;
    std::cout << "  w8 Total Bytes Sent: " << w8job2_ATPSocket->GetTotalTxBytes() << std::endl;
    std::cout << "  w9 Total Bytes Sent: " << w9job2_ATPSocket->GetTotalTxBytes() << std::endl;

    uint64_t job2TotalSent =
        w5job2_ATPSocket->GetTotalTxBytes() + w6job2_ATPSocket->GetTotalTxBytes() +
        w7job2_ATPSocket->GetTotalTxBytes() + w8job2_ATPSocket->GetTotalTxBytes() +
        w9job2_ATPSocket->GetTotalTxBytes();
    std::cout << "  Total Sent: " << job2TotalSent << std::endl;
    std::cout << "  Total Received: " << sinkApp2->GetTotalRxJob(job2Id) << std::endl;

    // Get atp l4 protocol
    Ptr<ATPL4Protocol> s0_atpl4 = staticRouting_s0->GetATPL4Protocol();
    Ptr<ATPL4Protocol> s1_atpl4 = staticRouting_s1->GetATPL4Protocol();
    Ptr<ATPL4Protocol> s2_atpl4 = staticRouting_s2->GetATPL4Protocol();
    Ptr<ATPL4Protocol> s3_atpl4 = staticRouting_s3->GetATPL4Protocol();

    // 输出哈希冲突次数
    std::cout << "\n=== 哈希冲突统计 ===" << std::endl;
    std::cout << "s0_atpl4 job1 hash collision counter: "
              << s0_atpl4->GetJobIdHashCollisionCounter(job1Id) << std::endl;

    std::cout << "s1_atpl4 job1 hash collision counter: "
              << s1_atpl4->GetJobIdHashCollisionCounter(job1Id) << std::endl;
    std::cout << "s1_atpl4 job2 hash collision counter: "
              << s1_atpl4->GetJobIdHashCollisionCounter(job2Id) << std::endl;

    std::cout << "s2_atpl4 job2 hash collision counter: "
              << s2_atpl4->GetJobIdHashCollisionCounter(job2Id) << std::endl;

    std::cout << "s3_atpl4 job1 hash collision counter: "
              << s3_atpl4->GetJobIdHashCollisionCounter(job1Id) << std::endl;
    std::cout << "s3_atpl4 job2 hash collision counter: "
              << s3_atpl4->GetJobIdHashCollisionCounter(job2Id) << std::endl;

    return 0;
}
