#include "atp-tx-buffer.h"
#include "atp-tag.h"
#include "ns3/log.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("ATPTxBuffer");
NS_OBJECT_ENSURE_REGISTERED(ATPTxBuffer);

TypeId
ATPTxBuffer::GetTypeId()
{
  static TypeId tid = TypeId("ns3::ATPTxBuffer")
    .SetParent<Object>()
    .SetGroupName("Internet")
    .AddConstructor<ATPTxBuffer>()
    .AddTraceSource("cwndTrace",
                    "Aggregator congestion window length",
                    MakeTraceSourceAccessor(&ATPTxBuffer::m_acwTrace),
                    "ns3::TracedValueCallback::Uint32");
  return tid;
}

ATPTxBuffer::ATPTxBuffer()
  : m_bufferSize(0), // 需要socket进行设置，比较重要
    m_bufferDataSize(0),
    m_packetNum(0),
    // A²TP初始化
    m_acw(1),
    m_lcw(1),
    m_alpha(0.0),
    m_beta(0.0),
    m_gamma(0.0),
    m_H(0.15),              // 论文推荐阈值
    m_virtualAcw(1.0),
    m_virtualLcw(1.0),
    m_packetsSentInPeriod(0),
    m_packetsToAggInPeriod(0),
    m_ecnCountInPeriod(0),
    m_collisionCountInPeriod(0),
    m_ackCountInPeriod(0),
    m_ackCountInPeriodLast(0)
{
  NS_LOG_FUNCTION(this);
}

ATPTxBuffer::~ATPTxBuffer()
{
    NS_LOG_FUNCTION(this);
    
    // 取消定时器
    if (m_windowIncreaseEvent.IsPending()) {
        m_windowIncreaseEvent.Cancel();
    }
    
    // 清理待发送队列
    while (!m_pendingQueue.empty())
    {
        ATPTxItem* item = m_pendingQueue.front();
        m_pendingQueue.pop();
        delete item;
    }
    
    // 清理已发送队列
    while (!m_sentQueue.empty())
    {
        ATPTxItem* item = m_sentQueue.front();
        m_sentQueue.pop();
        delete item;
    }

    // 清理重传队列
    while (!m_retxQueue.empty())
    {
        ATPTxItem* item = m_retxQueue.front();
        m_retxQueue.pop();
        delete item;
    }
}

void
ATPTxBuffer::SetMaxBufferSize(uint32_t size)
{
    m_bufferSize = size;
    m_maxCwnd = size / 248;
}

// 设置ACW（聚合器窗口）
void
ATPTxBuffer::SetACW(uint32_t acw)
{
    m_acw = acw;
    m_virtualAcw = static_cast<double>(acw);

    // 确保ACW ≤ LCW
    if (m_acw > m_lcw)
    {
        m_acw = m_lcw;
        m_virtualAcw = m_lcw;
    }
    m_acwTrace = m_acw;
}

// 设置LCW（链路窗口）
void
ATPTxBuffer::SetLCW(uint32_t lcw)
{
    m_lcw = lcw;
    m_virtualLcw = static_cast<double>(lcw);

    // 确保ACW ≤ LCW
    if (m_acw > m_lcw)
    {
        m_acw = m_lcw;
        m_virtualAcw = m_virtualLcw;
    }
}

void
ATPTxBuffer::SetStragglerWaitingThresholdUs(uint32_t thresholdUs)
{
    m_stragglerWaitingThresholdUs = thresholdUs;
}

void
ATPTxBuffer::SetStragglerSampleThreshold(uint32_t threshold)
{
    m_stragglerSampleThreshold = threshold;
}

// 获取ACW和LCW
uint32_t
ATPTxBuffer::GetACW() const
{
    return m_acw;
}

uint32_t
ATPTxBuffer::GetLCW() const
{
    return m_lcw;
}

bool
ATPTxBuffer::AddPacket(Ptr<Packet> p)
{
    NS_LOG_FUNCTION(this << p);
    
    if (p->GetSize() + m_bufferDataSize > m_bufferSize)
    {
        NS_LOG_WARN("Buffer full, packet dropped");
        return false;
    }
    
    // 添加数据包到缓冲区
    auto item = new ATPTxItem();
    item->m_packet = p->Copy();
    m_packetNum++; // 数据包序号
    item->m_packetId = m_packetNum;
    m_pendingQueue.push(item);
    m_bufferDataSize += p->GetSize();
    
    return true;
}

uint32_t
ATPTxBuffer::GetSentPacketId() const
{
    if (m_sentQueue.empty()) {
        NS_LOG_WARN("GetSentPacketId: m_sentQueue is empty");
        return 0;
    }

    return m_sentQueue.front()->m_packetId;
}

uint32_t
ATPTxBuffer::GetPendingFrontPacketId() const
{
    if (m_pendingQueue.empty()) {
        NS_LOG_WARN("GetPendingFrontPacketId: m_pendingQueue is empty");
        return 0;
    }

    return m_pendingQueue.front()->m_packetId;
}

uint32_t
ATPTxBuffer::GetCwndLeftBound() const
{
    return m_leftBound;
}

void
ATPTxBuffer::UpdateCwndLeftBound(uint32_t ackNum)
{
    NS_LOG_FUNCTION(this << ackNum);
    m_leftBound = ackNum;
}
Ptr<Packet>
ATPTxBuffer::SendPacket()
{
    NS_LOG_FUNCTION(this);

    // 检查是否有可用窗口
    if (m_pendingQueue.empty()) {
        NS_LOG_WARN("Buffer is empty, no packet to send");
        return nullptr;
    }

    // 发送新的数据包
    ATPTxItem* item = m_pendingQueue.front();
    m_pendingQueue.pop();
    m_bufferDataSize -= item->m_packet->GetSize();

    // 更新发送时间（微秒），将item存入已发送队列
    item->m_lastSentTime = static_cast<uint32_t>(Simulator::Now().GetMicroSeconds());
    m_sentQueue.push(item);
    // 记录发送统计
    m_packetsSentInPeriod++;
    m_highestSentPacketId = std::max(m_highestSentPacketId, item->m_packetId);

    return item->m_packet;
}

bool
ATPTxBuffer::IsOrderedAck(uint32_t packetId) const
{
    if (m_sentQueue.empty())
    {
        NS_LOG_WARN("IsOrderedAck: m_sentQueue is empty");
        return false;
    }

    ATPTxItem* item = m_sentQueue.front();
    uint32_t nextExpectedAckId = item->m_packetId;
    return packetId == nextExpectedAckId;
}

void
ATPTxBuffer::ProcessOrderedAck(uint32_t packetId)
{
    NS_LOG_FUNCTION(this << packetId);
    
    // 从m_sentQueue中移除已确认的包
    ATPTxItem* item = m_sentQueue.front();
    if (item->m_packetId == packetId)
    {
        m_sentQueue.pop();
        delete item;
    }
    else{  
        NS_LOG_WARN("Ordered ack, but packetId in m_sentQueue is not expected");
    }
}

Ptr<Packet>
ATPTxBuffer::ProcessUnorderedAck(uint32_t packetId)
{
    NS_LOG_FUNCTION(this << packetId);

    if (m_sentQueue.empty()) {
        NS_LOG_WARN("ProcessUnorderedAck: m_sentQueue is empty");
        return nullptr;
    }

    ATPTxItem* item = m_sentQueue.front();
    uint32_t nextExpectedAckId = item->m_packetId;

    if (packetId > nextExpectedAckId)
    {
        NS_LOG_WARN("Unordered ack received: " << packetId << ", expected: " << nextExpectedAckId);
        PacketQueue tempQueue;
        bool found = false;

        // 遍历队列找出所有需要重传的包
        while (!m_sentQueue.empty()) {
            ATPTxItem* front = m_sentQueue.front();
            m_sentQueue.pop();
            
            if (front->m_packetId == packetId) {
                // 找到匹配的包，删除它
                delete front;
                found = true;
            } else if (front->m_packetId < packetId) {
                // 创建新的重传项
                ATPTxItem* retxItem = new ATPTxItem();
                retxItem->m_packet = front->m_packet->Copy();
                retxItem->m_packetId = front->m_packetId;
                retxItem->m_lastSentTime = front->m_lastSentTime;
                m_retxQueue.push(retxItem);
            } else {
                // ID大于收到的ACK ID，保留
                tempQueue.push(front);
            }
        }

        // 将所有保留的包按原顺序放回m_sentQueue
        while (!tempQueue.empty()) {
            m_sentQueue.push(tempQueue.front());
            tempQueue.pop();
        }

        if (!found) {
            NS_LOG_WARN("ProcessUnorderedAck: Packet with ID " << packetId << " not found in sent queue");
        }
    }
    else{
        NS_LOG_WARN("Unordered ack, but packetId is less than nextExpectedAckId");
    }

    return item->m_packet;
}

void
ATPTxBuffer::CountCongestion(int type)
{
    NS_LOG_FUNCTION(this << type);

    if (type == 1 || type == 3)
    {
        m_ecnCountInPeriod++;
    }
    if (type == 2 || type == 3)
    {
        m_collisionCountInPeriod++;
    }
    m_ackCountInPeriod++;
}

void
ATPTxBuffer::UpdateStraggler(uint32_t stragglerBitmap0, uint32_t stragglerBitmap1, uint64_t waitingTime, Time rtt)
{
    double g = 1.0 / 16;
    double gamma = (MicroSeconds(waitingTime) / rtt).GetDouble();
    if (m_gamma == 0)
    {
        m_gamma = gamma;
    }
    else
    {
        m_gamma = gamma * g + m_gamma * (1 - g);
    }
    if (waitingTime <= m_stragglerWaitingThresholdUs)
    {
        return;
    }
    // NS_LOG_ERROR(waitingTime << " " << rtt << " " << stragglerBitmap0 << " " << stragglerBitmap1);

    std::pair<uint32_t, uint32_t> pos = std::make_pair(stragglerBitmap0, stragglerBitmap1);
    auto it = m_stragglers.find(pos);

    m_stragglerCount++;
    if (it == m_stragglers.end())
    {
        m_stragglers[pos] = 1;
        if (m_stragglerCountMax < 1)
        {
            m_stragglerCountMax = 1;
            m_stragglerBitmap0 = stragglerBitmap0;
            m_stragglerBitmap1 = stragglerBitmap1;
        }
    }
    else
    {
        it->second++;
        if (m_stragglerCountMax < it->second)
        {
            m_stragglerCountMax = it->second;
            m_stragglerBitmap0 = stragglerBitmap0;
            m_stragglerBitmap1 = stragglerBitmap1;
        }
    }
}

void
ATPTxBuffer::ProcessCongestion(uint32_t faninDegree, uint32_t bitmap0, uint32_t bitmap1, uint32_t packetId)
{
    // NS_LOG_ERROR(faninDegree);
    if (m_roundEndPacketId == 0)
    {
        m_roundEndPacketId = m_highestSentPacketId;
    }
    if (packetId < m_roundEndPacketId)
    {
        return;
    }
    else
    {
        // 只要收到的 ACK 序列号达到了我们标记的终点，本轮宣告结束
        m_roundEndPacketId = m_highestSentPacketId;
    }
    // 计算统计量
    double ecnRate = 0.0; // ECN标记比例
    double collisionRate = 0.0; // 哈希碰撞比例

    if (m_ackCountInPeriod > 0)
    {
        ecnRate = static_cast<double>(m_ecnCountInPeriod) / m_ackCountInPeriod;
        collisionRate = static_cast<double>(m_collisionCountInPeriod) / m_ackCountInPeriod;
    }

    double g = 1.0 / 16;
    m_beta = ecnRate * g + m_beta * (1 - g);
    m_alpha = collisionRate * g + m_alpha * (1 - g);

    if (ecnRate > 0)
    {
        double C = m_alpha * (faninDegree - 1);
        double rate = 1 - m_beta / 2 * (1 + C) / (1 + 2 * C);
        m_virtualAcw *= rate;
        if (m_virtualAcw < 1.0)
        {
            m_virtualAcw = 1.0;
        }
    }
    else
    {
        m_virtualAcw += static_cast<double>(m_ackCountInPeriod) / m_acw;
    }

    if (m_stragglerCount >= m_stragglerSampleThreshold)
    {
        bool isStraggler = (bitmap0 == m_stragglerBitmap0) && (bitmap1 == m_stragglerBitmap1);
        if (static_cast<double>(m_stragglerCountMax) / m_stragglerCount > 0.9 && !isStraggler)
        {
            m_virtualAcw *= (1 - m_gamma / 2);
        }
        m_stragglerBitmap0 = 0;
        m_stragglerBitmap1 = 0;
        m_stragglerCount = 0;
        m_stragglerCountMax = 0;
        m_stragglers.clear();
    }

    m_acw = static_cast<uint32_t>(std::max(1.0, m_virtualAcw));
    if (m_acw > m_maxCwnd)
    {
        m_acw = m_maxCwnd;
        // Ensure virtualCwnd doesn't exceed maxCwnd representation
        m_virtualAcw = static_cast<double>(m_maxCwnd);
    }
    m_acwTrace = m_acw;
    m_lcw = m_acw;

    m_packetsSentInPeriod = 0;
    m_packetsToAggInPeriod = 0;
    m_ecnCountInPeriod = 0;
    m_collisionCountInPeriod = 0;
    m_ackCountInPeriod = 0;
    m_ackCountInPeriodLast = 0;
}

bool
ATPTxBuffer::HasPacketToRetransmit() const
{
    return !m_retxQueue.empty();
}

Ptr<Packet>
ATPTxBuffer::RetransmitPacket()
{
    NS_LOG_FUNCTION(this);

    if (m_retxQueue.empty()) {
        NS_LOG_WARN("RetransmitPacket: m_retxQueue is empty");
        return nullptr;
    }

    ATPTxItem* item = m_retxQueue.front();
    m_retxQueue.pop();
    Ptr<Packet> packet = item->m_packet;
    delete item;

    return packet;
}

uint32_t
ATPTxBuffer::CheckAndMoveTimeoutPackets(uint32_t timeoutUs)
{
    NS_LOG_FUNCTION(this << timeoutUs);
    
    if (m_sentQueue.empty()) {
        return 0;
    }
    
    uint32_t currentTime = static_cast<uint32_t>(Simulator::Now().GetMicroSeconds());
    uint32_t timeoutCount = 0;
    PacketQueue tempQueue;
    
    // 遍历已发送队列，检查超时的包
    while (!m_sentQueue.empty()) {
        ATPTxItem* item = m_sentQueue.front();
        m_sentQueue.pop();
        
        uint32_t elapsedTime = currentTime - item->m_lastSentTime;
        
        if (elapsedTime >= timeoutUs) {
            // 超时了，移到重传队列
            NS_LOG_INFO("Packet " << item->m_packetId << " timeout, elapsed: " 
                        << elapsedTime << "us, threshold: " << timeoutUs << "us");

            ATPTxItem* retxItem = new ATPTxItem();
            retxItem->m_packet = item->m_packet->Copy();
            // 设置 ATPTag 的 resend = 1
            ATPTag atpTag;
            if (retxItem->m_packet->PeekPacketTag(atpTag)) {
                atpTag.m_resend = 1;
                retxItem->m_packet->ReplacePacketTag(atpTag);
            }
            retxItem->m_packetId = item->m_packetId;
            retxItem->m_lastSentTime = item->m_lastSentTime;
            m_retxQueue.push(retxItem);

            tempQueue.push(item);
            timeoutCount++;
        } else {
            // 没有超时，保留在队列中
            tempQueue.push(item);
        }
    }
    
    // 将未超时的包放回已发送队列
    while (!tempQueue.empty()) {
        m_sentQueue.push(tempQueue.front());
        tempQueue.pop();
    }
    
    if (timeoutCount > 0) {
        NS_LOG_WARN("Found " << timeoutCount << " timeout packets");
    }
    
    return timeoutCount;
}

} // namespace ns3
