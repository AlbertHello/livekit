# LiveKit SFU 核心架构分析 (CC + Pacer + Simulcast) - 全链路源码级解析

本文档针对 LiveKit SFU 后端的 CC (Congestion Control)、Pacer 和自适应 Simulcast 切档机制，进行深入源码级分析。核心覆盖：

- **Send-Side BWE**：基于 JitterPath 论文的简化版拥塞检测算法（非 GoogCC）
- **Stream Allocator**：多 Track 带宽分配与 Probe 探测机制
- **Forwarder**：RTP 包转换、层选择、Seq 重写、NTP 时间戳对齐
- **Video Layer Selector**：Simulcast 空间层切换
- **Pacer**：Leaky Bucket 发包控制

---

## 1. 架构全景图

LiveKit 的带宽管理和层切换经历了完全自研的演进，**废弃了 GoogCC，自研了一套基于 JitterPath 论文的 Send-Side BWE**。整体架构如下图：

```
                      ┌──────────────────────────────────────────┐
                      │         Stream Allocator                 │
                      │  ┌──────┐  ┌──────┐  ┌──────┐          │
                      │  │Track A│  │Track B│  │Track C│  ...    │
                      │  └──┬───┘  └──┬───┘  └──┬───┘          │
                      │     │          │          │               │
                      │     ▼          ▼          ▼               │
                      │  ┌──────────────────────────────┐        │
                      │  │        Forwarder             │        │
                      │  │  (AllocateOptimal,           │        │
                      │  │   ProvisionalAllocate, etc)  │        │
                      │  └──────────────┬───────────────┘        │
                      └─────────────────┼────────────────────────┘
                                        │
     ┌──────────────────────────────────┼──────────────────────────────┐
     │                    DownTrack (per subscriber track)             │
     │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
     │  │  RTPMunger   │  │VideoLayerSel │  │   Codec Munger       │  │
     │  │ (Seq Rewrite) │  │ (simulcast)  │  │ (VP8 TL filter, etc) │  │
     │  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
     │         │                 │                       │              │
     │         ▼                 ▼                       ▼              │
     │  ┌──────────────────────────────────────────────────────────┐   │
     │  │                    Pacer                                  │   │
     │  │  ┌────────────────┐  ┌─────────────────────────────┐    │   │
     │  │  │  Leaky Bucket  │  │  Base (TWCC + AbsSendTime)  │    │   │
     │  │  └───────┬────────┘  └──────────────┬──────────────┘    │   │
     │  └──────────┼──────────────────────────┼───────────────────┘   │
     └─────────────┼──────────────────────────┼───────────────────────┘
                   │                          │
                   ▼                          ▼
          ┌──────────────┐          ┌──────────────────────┐
          │  UDP Write   │          │  Send-Side BWE       │
          │              │          │  (JitterPath based)   │
          └──────────────┘          │  ┌────────────────┐  │
                                    │  │CongestionDetect│  │
                                    │  │  - QD measure  │  │
                                    │  │  - Loss measure│  │
                                    │  │  - CTR trend   │  │
                                    │  └───────┬────────┘  │
                                    └──────────┼───────────┘
                                               │
                                    ┌──────────▼───────────┐
                                    │  BWEListener         │
                                    │  OnCongestionState   │
                                    │  Change()            │
                                    └──────────────────────┘
```

---

## 2. Send-Side BWE：基于 JitterPath 的自研 CC 算法

### 2.1 设计哲学：为什么不用 GoogCC？

LiveKit 最初也使用了 GoogCC（通过 pion/interceptor），但在实践中发现 GoogCC 在 SFU 场景下有几个问题：
- GoogCC 是为端到端设计的，在 SFU 转发场景下不够精准
- 无法很好地与 SFU 的 Stream Allocator 配合
- 探测（Probing）机制不够灵活

因此 LiveKit 自研了基于 **JitterPath 论文**（[paper](https://homepage.iis.sinica.edu.tw/papers/lcs/2114-F.pdf)）的 Send-Side BWE。

### 2.2 核心概念：JQR / DQR / 传播排队延迟

在 [send_side_bwe.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/send_side_bwe.go#L29-L54) 中：

```go
// Based on a simplified/modified version of JitterPath paper
// TWCC feedback is used to calculate delta one-way-delay.
// It is accumulated/propagated to determine in which region
// groups of packets are operating in.
//
//   o JQR (Join Queuing Region) is when channel is congested.
//   o DQR (Disjoint Queuing Region) is when channel is not.
```

核心思想：
- 通过 TWCC (Transport-Wide Congestion Control) 反馈，计算每个包的**单向延迟变化**（delta one-way-delay）
- 将包按组聚合，计算每组的**传播排队延迟**（Propagated Queuing Delay）
- 根据传播排队延迟和丢包率判断当前处于 JQR（拥塞）还是 DQR（非拥塞）

### 2.3 拥塞状态机：三级状态

在 [bwe.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/bwe.go#L57-L76) 中定义了三个拥塞状态：

```go
type CongestionState int

const (
    CongestionStateNone         CongestionState = iota  // 无拥塞
    CongestionStateEarlyWarning                           // 早期预警
    CongestionStateCongested                              // 拥塞确认
)
```

状态转换在 [congestion_detector.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/congestion_detector.go#L951-L1001) 的 `congestionDetectionStateMachine` 中：

```go
func (c *congestionDetector) congestionDetectionStateMachine() (...) {
    switch fromState {
    case bwe.CongestionStateNone:
        if c.updateEarlyWarningSignal() == queuingRegionJQR {
            toState = bwe.CongestionStateEarlyWarning  // None → EarlyWarning
        }

    case bwe.CongestionStateEarlyWarning:
        if c.updateCongestedSignal() == queuingRegionJQR {
            toState = bwe.CongestionStateCongested      // EarlyWarning → Congested
        } else if c.updateEarlyWarningSignal() == queuingRegionDQR {
            toState = bwe.CongestionStateNone           // EarlyWarning → None (恢复)
        }

    case bwe.CongestionStateCongested:
        if c.updateCongestedSignal() == queuingRegionDQR {
            toState = bwe.CongestionStateNone           // Congested → None (恢复)
        }
    }
}
```

**关键设计**：早期预警（EarlyWarning）和拥塞确认（Congested）使用**不同的阈值**，形成滞回（Hysteresis），避免状态抖动。

### 2.4 阈值配置：EarlyWarning vs Congested

在 [congestion_detector.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/congestion_detector.go#L41-L81) 中：

| 信号类型 | EarlyWarning 阈值 | Congested 阈值 |
|----------|------------------|----------------|
| **排队延迟 JQR** | 2 groups, 200ms | 4 groups, 400ms |
| **排队延迟 DQR** | 3 groups, 300ms | 5 groups, 500ms |
| **丢包 JQR** | 3 groups, 300ms | 6 groups, 600ms |
| **丢包 DQR** | 4 groups, 400ms | 6 groups, 600ms |

**这意味着**：EarlyWarning 比 Congested 更容易触发（阈值更低），但状态机从 EarlyWarning 恢复到 None 也比从 Congested 恢复更容易。这形成了**快速预警 + 稳定确认**的双层设计。

### 2.5 Packet Group：分组聚合

包不是逐个判断拥塞的，而是按组（Packet Group）聚合。在 [packet_group.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/packet_group.go#L35-L45) 中：

```go
type PacketGroupConfig struct {
    MinPackets        int           // 最少 30 个包
    MaxWindowDuration time.Duration // 最长 500ms 窗口
}
```

一个 Packet Group 在以下条件之一满足时 Finalized：
1. 累计确认包数达到 `MinPackets`（30 个）
2. 窗口时间超过 `MaxWindowDuration`（500ms）

### 2.6 传播排队延迟的计算

在 [packet_group.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/packet_group.go#L160-L248) 中：

```go
func (p *packetGroup) Add(pi *packetInfo, sendDelta, recvDelta int64, isLost bool) error {
    // 累计发送间隔和接收间隔
    p.aggregateSendDelta += sendDelta
    p.aggregateRecvDelta += recvDelta
    // ...
}

func (p *packetGroup) PropagatedQueuingDelay() int64 {
    // 继承上一组的排队延迟 + 本组的 (recv_delta - send_delta)
    if p.inheritedQueuingDelay + p.aggregateRecvDelta - p.aggregateSendDelta > 0 {
        return p.inheritedQueuingDelay + p.aggregateRecvDelta - p.aggregateSendDelta
    }
    return max(0, p.aggregateRecvDelta - p.aggregateSendDelta)
}
```

**核心公式**：
```
propagated_queuing_delay = max(0, inherited_delay + aggregate_recv_delta - aggregate_send_delta)
```

- `inherited_delay`：上一组的传播排队延迟（延迟传播）
- `aggregate_recv_delta`：接收端包到达间隔之和
- `aggregate_send_delta`：发送端包发送间隔之和

如果接收间隔 > 发送间隔，说明包在网络上排队了，延迟增大。

### 2.7 QD Measurement（排队延迟测量）

在 [congestion_detector.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/congestion_detector.go#L181-L325) 中：

```go
type qdMeasurement struct {
    jqrConfig              CongestionSignalConfig  // JQR 触发条件
    dqrConfig              CongestionSignalConfig  // DQR 触发条件
    jqrMinDelay            int64   // 50ms, 进入 JQR 的最小延迟
    jqrMinTrendCoefficient float64 // 0.8, 延迟上升趋势的最小 Kendall 系数
    dqrMaxDelay            int64   // 20ms, 确认 DQR 的最大延迟

    propagatedQueuingDelays []int64  // 各组传播延迟
    // ...
}
```

**关键判断逻辑**：
- 如果 `propagated_queuing_delay > jqrMinDelay(50ms)`，该组进入 JQR
- 如果 `propagated_queuing_delay < dqrMaxDelay(20ms)`，该组进入 DQR
- 在 20ms-50ms 之间为 Indeterminate（不确定）

**趋势检测**：使用 Kendall's tau 趋势系数（trendCoefficient）检测延迟是否在上升：

```go
func (q *qdMeasurement) trendCoefficient() float64 {
    // 计算 concordant/discordant pairs
    // 如果延迟持续上升 (concordant pairs 多)，trendCoefficient > 0.8
    // 触发 JQR 判定需要同时满足：延迟 > 50ms AND 趋势系数 > 0.8
}
```

### 2.8 Loss Measurement（丢包测量）

在 [congestion_detector.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/congestion_detector.go#L329-L433) 中：

```go
type lossMeasurement struct {
    jqrMinLoss float64  // 0.25 (25% 加权丢包率)
    dqrMaxLoss float64  // 0.10 (10% 加权丢包率)
    // ...
}
```

- 加权丢包率 > 25% → JQR（拥塞）
- 加权丢包率 < 10% → DQR（非拥塞）

### 2.9 拥塞信号综合判定

在 [congestion_detector.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/congestion_detector.go#L881-L931) 的 `updateCongestionSignal` 中：

```go
qr := queuingRegionIndeterminate
qdQueuingRegion := c.qdMeasurement.QueuingRegion()
lossQueuingRegion := c.lossMeasurement.QueuingRegion()
switch {
case qdQueuingRegion == queuingRegionJQR:
    qr = queuingRegionJQR
    c.congestionReason = congestionReasonQueuingDelay  // 排队延迟导致的拥塞
case lossQueuingRegion == queuingRegionJQR:
    qr = queuingRegionJQR
    c.congestionReason = congestionReasonLoss          // 丢包导致的拥塞
case qdQueuingRegion == queuingRegionDQR && lossQueuingRegion == queuingRegionDQR:
    qr = queuingRegionDQR                               // 两者都确认非拥塞
    c.congestionReason = congestionReasonNone
}
```

**排队延迟优先于丢包**：如果排队延迟检测到 JQR，即使丢包检测还在 Indeterminate，也判定为拥塞。

### 2.10 可用信道容量估算

在 [congestion_detector.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/congestion_detector.go#L1068-L1111) 的 `estimateAvailableChannelCapacity` 中：

```go
func (c *congestionDetector) estimateAvailableChannelCapacity() {
    // 当拥塞时，使用 contributing groups（导致拥塞的那些组）
    // 否则，使用时间窗口（1s）的测量
    switch c.congestionReason {
    case congestionReasonQueuingDelay:
        minGroupIdx, maxGroupIdx = c.qdMeasurement.GroupRange()
    case congestionReasonLoss:
        minGroupIdx, maxGroupIdx = c.lossMeasurement.GroupRange()
    default:
        useWindow = true  // 使用 1s 窗口
    }
    // ...
    c.estimatedAvailableChannelCapacity = agg.AcknowledgedBitrate()
}
```

### 2.11 CTR (Captured Traffic Ratio) 趋势监测

在拥塞状态下，LiveKit 还持续监测 CTR 趋势，用于发现初始估计不准确的情况：

在 [congestion_detector.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/bwe/sendsidebwe/congestion_detector.go#L1146-L1156) 中：

```go
// 当进入拥塞状态时，启动 CTR 趋势监测
if state == bwe.CongestionStateCongested && fromState != bwe.CongestionStateCongested {
    c.createCTRTrend()
}

// 在拥塞状态下，如果 CTR 趋势下降，说明估计偏高，下调估计值
if c.congestedCTRTrend != nil && c.congestedCTRTrend.GetDirection() == ccutils.TrendDirectionDownward {
    congestedAckedBitrate := c.congestedTrafficStats.AcknowledgedBitrate()
    if congestedAckedBitrate < c.estimatedAvailableChannelCapacity {
        c.estimatedAvailableChannelCapacity = congestedAckedBitrate
        shouldNotify = true
    }
}
```

---

## 3. Stream Allocator：带宽分配中枢

### 3.1 整体架构

Stream Allocator 是 LiveKit 带宽管理的核心，它连接了 BWE、Pacer 和各个 DownTrack。在 [streamallocator.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/streamallocator/streamallocator.go) 中：

```go
type StreamAllocator struct {
    params StreamAllocatorParams  // BWE + Pacer + RTTGetter
    bwe    bwe.BWE                // 带宽估计器

    committedChannelCapacity  int64  // 已确认的信道容量

    videoTracks   map[livekit.TrackID]*Track  // 管理的视频 Track
    prober        *ccutils.Prober             // 探针管理器

    state         streamAllocatorState  // STABLE / DEFICIENT
    activeProbeClusterId  ccutils.ProbeClusterId  // 活跃探针
}
```

### 3.2 两种状态：STABLE vs DEFICIENT

在 [streamallocator.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/streamallocator/streamallocator.go#L63-L79) 中：

```go
const (
    streamAllocatorStateStable    streamAllocatorState = iota  // 所有 Track 都满足
    streamAllocatorStateDeficient                               // 有 Track 带宽不足
)
```

状态切换逻辑在 [streamallocator.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/streamallocator/streamallocator.go#L920-L929) 中：

```go
func (s *StreamAllocator) adjustState() {
    for _, track := range s.getVideoTracks() {
        if track.IsDeficient() {
            s.setState(streamAllocatorStateDeficient)  // 任一 Track 不足 → DEFICIENT
            return
        }
    }
    s.setState(streamAllocatorStateStable)
}
```

### 3.3 事件驱动模型

Stream Allocator 使用事件队列处理所有操作，确保线程安全：

```go
// 事件类型
streamAllocatorSignalAllocateTrack       // 分配单个 Track
streamAllocatorSignalAllocateAllTracks   // 分配所有 Track
streamAllocatorSignalEstimate            // REMB 估计（或 send-side BWE 回调）
streamAllocatorSignalFeedback            // TWCC 反馈
streamAllocatorSignalPeriodicPing        // 定时 Ping（30s 长间隔 or 100ms 短间隔）
streamAllocatorSignalCongestionStateChange // 拥塞状态变化
streamAllocatorSignalSendProbe           // 发送探测包
// ...
```

### 3.4 拥塞状态变化的处理

在 [streamallocator.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/streamallocator/streamallocator.go#L856-L897) 中：

```go
func (s *StreamAllocator) handleSignalCongestionStateChange(event Event) {
    cscd := event.congestionStateChangeData

    if cscd.toState != bwe.CongestionStateNone {
        s.maybeStopProbe()  // 拥塞时停止探针
    }

    // 从 EarlyWarning 恢复到 None，且处于 STABLE 状态
    if isHoldableCongestionState(cscd.fromState) &&
       cscd.toState == bwe.CongestionStateNone &&
       s.state == streamAllocatorStateStable {
        // 释放被 hold 的 Track，分配最优层
        update := NewStreamStateUpdate()
        for _, track := range s.getVideoTracks() {
            allocation := track.AllocateOptimal(cFlagAllowOvershootWhileOptimal, false)
            updateStreamStateChange(track, allocation, update)
        }
        s.maybeSendUpdate(update)
    }

    // 进入 Congested 状态
    if cscd.toState == bwe.CongestionStateCongested {
        if s.activeProbeClusterId != ccutils.ProbeClusterIdInvalid {
            // 探针期间拥塞，不更新 channel capacity
            s.activeProbeCongesting = true
        } else {
            // 更新 channel capacity 并重新分配
            s.committedChannelCapacity = cscd.estimatedAvailableChannelCapacity
            s.allocateAllTracks()
        }
    }
}
```

**关键行为**：
- **EarlyWarning** 时**不立即降档**，而是"hold"住（hold = true），等待确认
- **Congested** 时才更新 `committedChannelCapacity` 并触发全量重分配
- 如果在探针期间发生拥塞，不更新 channel capacity，避免探针流量造成的误判

### 3.5 全量分配算法：allocateAllTracks

这是带宽分配的核心，在 [streamallocator.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/streamallocator/streamallocator.go#L1149-L1235) 中：

```go
func (s *StreamAllocator) allocateAllTracks() {
    // 1. 先给非受管 Track（如屏幕共享单层）分配最优层
    for _, track := range videoTracks {
        if !track.IsManaged() {
            allocation := track.AllocateOptimal(...)
            availableChannelCapacity -= allocation.BandwidthRequested
        }
    }

    // 2. 按层从低到高逐层分配
    //    按优先级排序的 Track，每个 Track 在每个 (spatial, temporal) 层
    //    尝试分配，直到带宽耗尽
    for spatial := int32(0); spatial <= buffer.DefaultMaxLayerSpatial; spatial++ {
        for temporal := int32(0); temporal <= buffer.DefaultMaxLayerTemporal; temporal++ {
            layer := buffer.VideoLayer{Spatial: spatial, Temporal: temporal}
            for _, track := range sorted {
                _, used := track.ProvisionalAllocate(availableChannelCapacity, layer, ...)
                availableChannelCapacity -= used
            }
        }
    }
}
```

**分配策略**：
- 第一轮：非受管 Track（如屏幕共享）先拿走最优分配
- 第二轮：受管 Track 按优先级排序，从低层向高层逐层分配
- 每个 Track 在每个层只分配一次，保证公平性

### 3.6 单个 Track 分配：allocateTrack

在 [streamallocator.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/streamallocator/streamallocator.go#L931-L1085) 中：

```go
func (s *StreamAllocator) allocateTrack(track *Track) {
    // 如果状态是 STABLE，直接分配最优层
    if s.state == streamAllocatorStateStable && !isDeficientCongestionState(bweCongestionState) {
        allocation := track.AllocateOptimal(...)
        return
    }

    // DEFICIENT 状态下的协同分配
    // 1. 获取协同过渡 (Cooperative Transition)
    transition := track.ProvisionalAllocateGetCooperativeTransition(...)

    // 如果降档（释放带宽），提交并 boost 其他 deficient track
    if transition.From.GreaterThan(transition.To) {
        allocation := track.ProvisionalAllocateCommit()
        s.maybeBoostDeficientTracks()  // 用释放的带宽帮助其他 track
        return
    }

    // 2. 如果需要更多带宽，尝试用 headroom 分配
    availableChannelCapacity := s.getAvailableHeadroomWithoutTracks(false, []*Track{track})
    // 从低到高遍历所有层，找到能 fit 的最高层
    for spatial := int32(0); spatial <= buffer.DefaultMaxLayerSpatial; spatial++ {
        for temporal := int32(0); temporal <= buffer.DefaultMaxLayerTemporal; temporal++ {
            isCandidate, used := track.ProvisionalAllocate(availableChannelCapacity, layer, ...)
            if isCandidate { bestLayer = layer }
        }
    }

    // 3. 如果 headroom 不够，从其他 track 借带宽
    //    按 MinDistance 排序（距离 desired 最近的 track 优先贡献）
    for _, t := range minDistanceSorted {
        tx := t.ProvisionalAllocateGetBestWeightedTransition()
        if tx.BandwidthDelta < 0 {  // 释放带宽
            contributingTracks = append(contributingTracks, t)
            bandwidthAcquired += -tx.BandwidthDelta
        }
    }
}
```

### 3.7 探针（Probing）机制

当处于 DEFICIENT 状态时，Stream Allocator 会尝试探测网络是否有更多带宽可用。

在 [streamallocator.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/streamallocator/streamallocator.go#L1320-L1382) 中：

```go
func (s *StreamAllocator) maybeProbe() {
    switch s.params.Config.ProbeMode {
    case ProbeModeMedia:
        // 媒体探测：直接 boost 一个 deficient track
        s.maybeProbeWithMedia()

    case ProbeModePadding:
        // Padding 探测：发送 padding-only RTP 包
        s.maybeProbeWithPadding()
    }
}

func (s *StreamAllocator) maybeProbeWithPadding() {
    for _, track := range s.getMaxDistanceSortedDeficientVideoTracks() {
        transition, available := track.GetNextHigherTransition(...)
        // 计算需要探测的带宽
        desiredIncreaseBps := max((transition.BandwidthDelta * probeOveragePct) / 100, probeMinBps)
        // 创建探针集群
        pci := s.prober.AddCluster(
            ProbeClusterModeUniform,
            ProbeClusterGoal{
                AvailableBandwidthBps: int(s.committedChannelCapacity),
                ExpectedUsageBps:      int(expectedBandwidthUsage),
                DesiredBps:            int(expectedBandwidthUsage + desiredIncreaseBps),
                Duration:              s.params.BWE.ProbeDuration(),
            },
        )
    }
}
```

**探针配置**：
- `ProbeOveragePct`: 120%（探测目标 = 当前需求 + 20% 余量）
- `ProbeMinBps`: 200,000 bps（最小探测 200kbps）
- `ProbeDuration`: 由 BWE 的 `ProbeRegulator` 控制

**探针生命周期**：

```
1. Prober.AddCluster() → OnProbeClusterSwitch 回调
2. StreamAllocator 通知 BWE: ProbeClusterStarting()
3. Pacer: StartProbeCluster() → 开始记录探针包的发送
4. Prober 定时触发: OnSendProbe(bytesToSend)
5. StreamAllocator 通过 Track.WriteProbePackets() 发送 padding RTP
6. BWE 通过 TWCC 反馈分析探针结果
7. Pacer: EndProbeCluster() → BWE.ProbeClusterDone()
8. StreamAllocator 根据 probeSignal 决定是否更新 channel capacity
```

---

## 4. Forwarder：层选择与分配的核心

### 4.1 核心数据结构

在 [forwarder.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/forwarder.go#L93-L252) 中：

```go
type VideoAllocation struct {
    PauseReason         VideoPauseReason  // NONE / MUTED / PUB_MUTED / BANDWIDTH / FEED_DRY
    IsDeficient         bool              // 是否带宽不足
    BandwidthRequested  int64             // 当前请求的带宽
    BandwidthDelta      int64             // 带宽变化量
    BandwidthNeeded     int64             // 最优层需要的带宽
    TargetLayer         buffer.VideoLayer // 目标层 {Spatial, Temporal}
    RequestLayerSpatial int32             // 请求的空间层
    DistanceToDesired   float64           // 距离 desired 层的距离
}

type VideoTransition struct {
    From           buffer.VideoLayer  // 当前层
    To             buffer.VideoLayer  // 目标层
    BandwidthDelta int64              // 带宽变化
}

type Forwarder struct {
    vls          videolayerselector.VideoLayerSelector  // 层选择器
    provisional  *VideoAllocationProvisional             // 临时分配状态
    lastAllocation VideoAllocation                      // 上次分配结果
    rtpMunger    *RTPMunger                             // RTP Seq 重写
    codecMunger  codecmunger.CodecMunger                // 编解码器处理
    // ...
}
```

### 4.2 Bitrates：按层码率查询

`Bitrates` 是一个二维数组 `[spatial][temporal]int64`，存储了从 Producer 侧实时统计的每层码率。在 `ProvisionalAllocate` 中，直接通过 `brs[layer.Spatial][layer.Temporal]` 查询该层需要的带宽。

**注意**：LiveKit 的 Simulcast 模式下，各空间层是**独立流**，所以 `brs[sp][tl]` 是**该流自己的单层码率，不需要累加低层**（与 SVC 的累加语义不同）。

### 4.3 AllocateOptimal：最优分配

在 [forwarder.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/forwarder.go#L802-L966) 中：

```go
func (f *Forwarder) AllocateOptimal(availableLayers []int32, brs Bitrates, allowOvershoot bool, hold bool) VideoAllocation {
    // 1. 计算最优带宽需求
    optimalBandwidthNeeded := getOptimalBandwidthNeeded(muted, pubMuted, maxSeenLayer.Spatial, brs, maxLayer)

    // 2. 找到最高可请求的层
    maxLayerSpatialLimit := min(maxLayer.Spatial, maxSeenLayer.Spatial)
    highestAvailableLayer := max available layer from availableLayers
    requestLayerSpatial := max layer ≤ maxLayerSpatialLimit

    // 3. 如果当前层有效
    if currentLayer.IsValid() {
        if hold {
            // hold 模式：只分配最低可用层 + temporal=0
            alloc.TargetLayer = {lowestAvailableLayer, 0}
        } else {
            // 非 hold：分配最高可用层
            alloc.TargetLayer = {requestLayerSpatial, maxTemporal}
        }
    } else {
        // 当前层无效：机会主义分配
        opportunisticAlloc()
    }
}
```

**关键行为**：
- `hold=true`（EarlyWarning 拥塞时）：**只分配最低层，temporal=0**，防止增加信道负载
- `hold=false`（正常或 Congested 后恢复）：分配最高可用层
- `allowOvershoot` 且 `IsOvershootOkay()`：可以分配超过 `maxLayer` 的层（机会主义）

### 4.4 ProvisionalAllocate：试探性分配

在 [forwarder.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/forwarder.go#L999-L1041) 中：

```go
func (f *Forwarder) ProvisionalAllocate(availableChannelCapacity int64, layer buffer.VideoLayer, allowPause bool, allowOvershoot bool) (bool, int64) {
    requiredBitrate := f.provisional.bitrates[layer.Spatial][layer.Temporal]
    if requiredBitrate == 0 { return false, 0 }

    alreadyAllocatedBitrate := get current allocated bitrate

    // 如果该层不超过 maxLayer，且 fit 进可用带宽
    if !layer.GreaterThan(f.provisional.maxLayer) && requiredBitrate <= (availableChannelCapacity + alreadyAllocatedBitrate) {
        f.provisional.allocatedLayer = layer
        return true, requiredBitrate - alreadyAllocatedBitrate
    }

    // 如果不允许 pause，即使不够也分配最低能用的层
    if !allowPause && (!f.provisional.allocatedLayer.IsValid() || !layer.GreaterThan(f.provisional.allocatedLayer)) {
        f.provisional.allocatedLayer = layer
        return true, requiredBitrate - alreadyAllocatedBitrate
    }
    return false, 0
}
```

### 4.5 ProvisionalAllocateGetCooperativeTransition：协同过渡

在 [forwarder.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/forwarder.go#L1043-L1176) 中：

```go
// 协同分配的目标：尽量保持所有 stream 活跃
// 1. 如果 mute → 释放所有带宽
// 2. 如果当前层有效且 ≤ 最大可用层 → 保持当前层（不升级）
// 3. 如果当前层 > 最大可用层 → 降级到最大可用层
// 4. 如果当前未流式传输 → 找最小可用层恢复
```

### 4.6 ProvisionalAllocateGetBestWeightedTransition：最佳加权过渡

在 [forwarder.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/forwarder.go#L1178-L1272) 中：

```go
// 计算"性价比"最高的降级方案
// value = bandwidthDelta / (transitionCost + qualityCost)
// transitionCost: 空间层切换 = 10, 时间层切换 = 0
// qualityCost: (maxTemporal+1) * spatialDelta + temporalDelta
```

**核心思想**：当需要从其他 Track 借带宽时，选择**节省带宽最多、画质损失最小**的降级方案。

### 4.7 AllocateNextHigher：逐级升档

在 [forwarder.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/forwarder.go#L1354-L1459) 中：

```go
// 升档顺序：
// 1. 先尝试在当前空间层内升时间层
//    targetLayer.Spatial, targetLayer.Temporal+1 → maxLayer.Temporal
// 2. 再尝试升空间层（从 t0 开始）
//    targetLayer.Spatial+1 → maxLayer.Spatial, 0 → maxLayer.Temporal
// 3. 如果允许 overshoot，尝试超过 maxLayer
```

**升档策略**：优先升时间层（无切换成本），再升空间层（需要关键帧）。

### 4.8 updateAllocation：提交分配并设置目标层

在 [forwarder.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/forwarder.go#L1595-L1619) 中：

```go
func (f *Forwarder) updateAllocation(alloc VideoAllocation, reason string) VideoAllocation {
    // 限制 H264 的 temporal 为 0（不支持时间层）
    if alloc.TargetLayer.IsValid() && f.mime == mime.MimeTypeH264 {
        alloc.TargetLayer.Temporal = 0
    }

    f.lastAllocation = alloc
    f.setTargetLayer(alloc.TargetLayer, alloc.RequestLayerSpatial)

    if !f.vls.GetTarget().IsValid() {
        f.resyncLocked()  // 目标层无效 → 重置同步状态
    }
    return f.lastAllocation
}
```

### 4.9 RTP 翻译与包处理

在 `getTranslationParamsVideo` 中（[forwarder.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/forwarder.go#L2090-L2127)）：

```go
// 关键降档保护：
if FlagPauseOnDowngrade && f.isDeficientLocked() &&
   f.vls.GetTarget().Spatial < f.vls.GetCurrent().Spatial {
    // 目标空间层低于当前空间层 + 处于 DEFICIENT 状态
    // → 直接丢弃当前层的高层包，不等关键帧！
    tp.shouldDrop = true
}
```

**这是与 Mediasoup 的 Pacer Bypass 同等重要的设计**：当拥塞导致降档时，**立即丢弃当前空间层的高层包**，不等关键帧也不排队，物理网络瞬间减压。

### 4.10 RTPMunger：序列号重写

在 [rtpmunger.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/rtpmunger.go) 中：

```go
type RTPMunger struct {
    // 管理下行包的序列号重写
    // 当包被丢弃时（如高层包丢弃、时间层过滤），更新 SN offset
    // 保证下行 SN 连续
}

func (r *RTPMunger) PacketDropped(extPkt *buffer.ExtPacket) {
    // 记录被丢弃的包，调整 offset
}

func (r *RTPMunger) UpdateAndGetSnTs(extPkt *buffer.ExtPacket) (...) {
    // 重写 SN 和 Timestamp
    // 考虑 discard padding（由于丢包造成的 SN 偏移）
}
```

### 4.11 NTP 时间戳跨流对齐

在 [forwarder.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/forwarder.go#L1721-L1749) 中：

```go
func (f *Forwarder) getRefLayerRTPTimestamp(ts uint32, refLayer, targetLayer int32) (uint32, error) {
    if refLayer == targetLayer || f.refVideoLayerMode == MULTIPLE_SPATIAL_LAYERS_PER_STREAM {
        return ts, nil  // 同一流或 SVC 模式，无需对齐
    }

    // Simulcast 模式：通过 NTP 时间戳对齐不同流的 RTP 时间戳
    srRef := f.refInfos[refLayer].senderReport
    srTarget := f.refInfos[targetLayer].senderReport

    ntpDiff := srRef.NtpTime - srTarget.NtpTime
    rtpDiff := ntpDiff * clockRate / 1e9

    // 计算两个流之间的 RTP 时间戳偏移
    offset := srRef.RtpTimestamp - (srTarget.RtpTimestamp + rtpDiff)
    return ts + offset, nil
}
```

---

## 5. Video Layer Selector：Simulcast 空间层切换

### 5.1 Simulcast Selector

在 [simulcast.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/videolayerselector/simulcast.go) 中：

```go
func (s *Simulcast) Select(extPkt *buffer.ExtPacket, layer int32) VideoLayerSelectorResult {
    if s.currentLayer.Spatial != s.targetLayer.Spatial {
        // 空间层不匹配，需要切换
        if extPkt.IsKeyFrame {
            if layer > s.currentLayer.Spatial && layer <= s.targetLayer.Spatial {
                // 升档：当前层 < 包层 ≤ 目标层
                reason = "upgrading layer"
                found = true
            }
            if layer < s.currentLayer.Spatial && layer >= s.targetLayer.Spatial {
                // 降档：目标层 ≤ 包层 < 当前层
                reason = "downgrading layer"
                found = true
            }
        }
    }

    // 处理 overshoot 回拨
    if s.currentLayer.Spatial > s.maxLayer.Spatial && layer <= s.maxLayer.Spatial && extPkt.IsKeyFrame {
        s.currentLayer.Spatial = layer
        populateSwitches(true, "adjusting overshoot")
    }
}
```

**关键行为**：
- 空间层切换**必须等待关键帧**（升档和降档都需要）
- 如果当前层超过了 maxLayer（overshoot），遇到关键帧时回拨
- 切换后更新 `previousLayer`、`previousTargetLayer` 以便追踪

### 5.2 IsOvershootOkay

```go
func (s *Simulcast) IsOvershootOkay() bool {
    return true  // Simulcast 允许 overshoot
}
```

Simulcast 允许 overshoot 是因为：各空间层是独立流，暂时发送超过订阅层的包不会造成解码问题（客户端可以忽略），但能提供更平滑的降档体验。

---

## 6. Pacer：Leaky Bucket 发包控制

### 6.1 三种 Pacer 模式

在 [pacer.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/pacer/pacer.go#L36-L42) 中：

```go
const (
    PacerBehaviorPassThrough PacerBehavior = "pass-through"  // 直通，无控制
    PacerBehaviorNoQueue     PacerBehavior = "no-queue"      // 不入队，仅记账
    PacerBehaviorLeakybucket PacerBehavior = "leaky-bucket"  // 漏桶控制
)
```

### 6.2 Leaky Bucket 实现

在 [leaky_bucket.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/pacer/leaky_bucket.go) 中：

```go
func (l *LeakyBucket) sendWorker() {
    interval := l.interval        // 发包间隔
    intervalBytes := int(interval.Seconds() * float64(bitrate) / 8.0)
    maxOvershootBytes := int(float64(intervalBytes) * maxOvershootFactor)  // 2x

    for {
        toSendBytes := intervalBytes - overage  // 本周期可发送字节数
        if toSendBytes < 0 {
            overage = -toSendBytes  // 超发太多，等下周期
            continue
        }
        if toSendBytes > maxOvershootBytes {
            toSendBytes = maxOvershootBytes  // 限制最大超发
        }

        // 从队列取包发送，直到额度用完
        for toSendBytes > 0 && queue not empty {
            p := l.packets.PopFront()
            written, _ := l.Base.SendPacket(p)
            toSendBytes -= written
        }
        overage = -toSendBytes  // 剩余额度留给下周期
    }
}
```

**与 Mediasoup 的关键区别**：LiveKit 的 Leaky Bucket **有队列**，包会先入队再由 sendWorker 定时发出。这与 Mediasoup 的 Pacer Bypass（零缓冲）不同。

### 6.3 Base Pacer：TWCC 与 AbsSendTime

在 [base.go](file:///Users/qzwang/Documents/Gitee/livekit/pkg/sfu/pacer/base.go#L92-L136) 中：

```go
func (b *Base) patchRTPHeaderExtensions(p *Packet) error {
    // 1. 更新 AbsSendTime（绝对发送时间）
    if p.AbsSendTimeExtID != 0 {
        absSendTimeExt.Timestamp = ToNtpTime(sendingAt) >> 14
    }

    // 2. 分配 TWCC 序列号
    if p.TransportWideExtID != 0 && b.bwe != nil {
        twccSN := b.bwe.RecordPacketSendAndGetSequenceNumber(
            sendingAt.UnixMicro(), packetSize, p.IsRTX, p.ProbeClusterId, p.IsProbe,
        )
        twccExt.TransportSequence = twccSN
    }

    // 3. 记录探针观察者
    b.ProbeObserver.RecordPacket(packetSize, p.IsRTX, p.ProbeClusterId, p.IsProbe)
}
```

**每个下行 Transport 有唯一的 TWCC 序列号空间**，所有 DownTrack 和探测包共享这个空间。

---

## 7. 完整全链路数据流

```
┌─[上行 RTP 包到达 SFU]────────────────────────────────────────────────┐
│  Producer/Receiver 接收 → 解析层信息 → 统计 Bitrates (每层码率)      │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─[DownTrack 收包]─────────────────────────────────────────────────────┐
│  ├─ Forwarder.GetTranslationParams()                                  │
│  │   ├─ VideoLayerSelector.Select()  → 判断层是否匹配                │
│  │   ├─ CodecMunger  → 时间层过滤 (VP8 TL)                          │
│  │   ├─ RTPMunger    → 序列号重写                                    │
│  │   └─ NTP 时间戳对齐 (Simulcast 跨流)                              │
│  └─ 如果包被接受 → 交给 Pacer                                        │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─[Pacer 发包]─────────────────────────────────────────────────────────┐
│  ├─ Leaky Bucket: 入队 → 定时发送                                    │
│  ├─ Base: patch AbsSendTime + TWCC 扩展                              │
│  └─ WriteRTP() → UDP 发送                                            │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─[TWCC 反馈循环]──────────────────────────────────────────────────────┐
│  客户端发 RTCP TransportFeedback                                      │
│  → StreamAllocator.handleSignalFeedback()                             │
│  → BWE.HandleTWCCFeedback(report)                                     │
│  → CongestionDetector: 计算 PacketGroup → 更新 QD/Loss Measurement   │
│  → congestionDetectionStateMachine() → 状态转换                      │
│  → OnCongestionStateChange() → StreamAllocator 处理                  │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─[拥塞处理]──────────────────────────────────────────────────────────┐
│  EarlyWarning: hold=true → AllocateOptimal(hold=true) → 最低层       │
│  Congested:    committedChannelCapacity = 估计值                     │
│                → allocateAllTracks() → 全量重分配                    │
│                → Forwarder: 降档 + FlagPauseOnDowngrade 立即丢包     │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─[恢复与探针]─────────────────────────────────────────────────────────┐
│  Congested → DQR → None: 状态恢复                                    │
│  DEFICIENT 状态: maybeProbe()                                        │
│    ├─ ProbeModePadding: 发送 padding RTP 探测                        │
│    └─ ProbeModeMedia: boost 一个 deficient track                     │
│  探针结果:                                                            │
│    ├─ NotCongesting + 容量足够 → 更新 committedChannelCapacity       │
│    └─ Congesting → 保持当前分配                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 8. 与 Mediasoup 的关键差异对比

| 维度 | LiveKit | Mediasoup |
|------|---------|-----------|
| **CC 算法** | 自研 JitterPath-based Send-Side BWE | GoogCC (WebRTC) |
| **拥塞状态** | 三级：None / EarlyWarning / Congested | 单级：BWE 数值变化 |
| **拥塞检测** | 排队延迟 + 丢包双路检测，Kendall 趋势分析 | GoogCC 内部（基于延迟和丢包） |
| **Pacer 模式** | Leaky Bucket（有队列） | Pacer Bypass（无队列，零缓冲） |
| **降档时机** | FlagPauseOnDowngrade：立即丢包 | 同等：立即丢包 + Seq 重写 |
| **升档防抖** | 无显式防抖，通过 EarlyWarning → Congested 两级状态实现滞回 | 10 秒 BweDowngradeConservativeMs |
| **带宽分配** | Stream Allocator 事件驱动，逐层分配 | Transport.DistributeAvailableOutgoingBitrate 两轮分配 |
| **探测机制** | Prober + ProbeRegulator，支持 Padding/Media 两种模式 | GoogCC 内置 ALR Probing |
| **公平性** | 优先级 + 最小距离 + 加权过渡 | 优先级 + 公平轮询 |
| **Simulcast 时间戳** | NTP 对齐（相同算法） | NTP 对齐 |
| **Seq 重写** | RTPMunger（offset 模式） | SeqManager（Input/Drop/Sync） |
| **Score 机制** | 无独立 Score（使用 IsDeficient） | 有（pow(deliveredRatio, 4) * 10） |
| **CTR 监测** | 有（拥塞期间持续监测 CTR 趋势） | 无 |
| **EarlyWarning** | 有（hold 模式，不立即降档，等待确认） | 无 |

---

## 9. 关键设计亮点总结

### 9.1 三级拥塞状态 + 滞回

LiveKit 的 None → EarlyWarning → Congested 三级状态机是其核心创新：

- **EarlyWarning**：快速检测到拥塞信号，但不立即降档，而是"hold"（分配最低层）。这避免了因瞬时抖动导致的频繁切换。
- **Congested**：确认拥塞后才更新 channel capacity 并全量重分配。
- **滞回**：EarlyWarning 和 Congested 使用不同的阈值，恢复也需要经过 DQR 确认，避免状态振荡。

### 9.2 排队延迟 + 丢包双路检测

不同于 GoogCC 的黑盒模型，LiveKit 的拥塞检测是**可解释的**：

- **排队延迟路径**：通过 TWCC 反馈计算 `propagated_queuing_delay`，用 Kendall 趋势分析检测延迟上升
- **丢包路径**：用加权丢包率（Weighted Loss）作为辅助判断
- 两者独立判断，排队延迟优先

### 9.3 CTR 趋势作为安全网

在拥塞期间持续监测 CTR（Captured Traffic Ratio），如果 CTR 趋势下降，说明初始估计偏高，自动下调 `estimatedAvailableChannelCapacity`。这是一个**自校正机制**，防止初始估计不准确导致的持续拥塞。

### 9.4 FlagPauseOnDowngrade：即时降档

与 Mediasoup 的 Pacer Bypass 同等重要：当 `IsDeficient && targetLayer.Spatial < currentLayer.Spatial` 时，**立即丢弃当前层的高层包**，不等待关键帧，不给 Pacer 队列积压的机会。

### 9.5 协同流分配

`ProvisionalAllocateGetCooperativeTransition` 和 `ProvisionalAllocateGetBestWeightedTransition` 实现了**多 Track 间的协同降级**，通过"性价比"（bandwidth saved / cost）计算最优降级方案，确保带宽分配公平且高效。

---

## 10. 给你的自研 SFU 建议

1. **CC 算法选择**：LiveKit 的自研 JitterPath BWE 证明了脱离 GoogCC 是可行的，且可以做得更好（更可解释、更可控）。关键是排队延迟 + 丢包的双路检测。

2. **状态机设计**：三级状态（None / EarlyWarning / Congested）比单一 BWE 数值更灵活，EarlyWarning 的"hold"机制是防止抖动的好方法。

3. **Pacer 的选择**：LiveKit 选择了 Leaky Bucket（有队列），Mediasoup 选择了 Pacer Bypass（无队列）。两者各有优劣——Leaky Bucket 更平滑但可能积压，Pacer Bypass 零延迟但可能突发。选择取决于你的场景需求。

4. **降档响应**：无论哪种 Pacer，降档时**立即丢弃高层包**是关键。不要让已经决定丢弃的包继续占用物理带宽。

5. **探测机制**：Padding 探测比 Media 探测更可控，推荐使用。探测时应设置 `ProbeOveragePct`（如 120%）以避免低估。

6. **协同分配**：多 Track 场景下，简单的公平轮询不如 LiveKit 的"加权性价比"降级方案高效。后者能更好地平衡带宽节省和画质损失。