use std::collections::{BTreeSet, HashSet};
use std::io::Cursor;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::time::Duration;

use binrw::{BinRead, BinWrite};
use serde::Serialize;

use syncdaq::ctrl_msg::{CmdReplySummary, CtrlMsg, send_cmd};
use syncdaq::default_cfg::DEFAULT_CTRL_PORT;
use syncdaq::device_discovery::{self, DeviceInfo};

/// 一次发现最多单播多少个目标（本机广播地址 + 各配置网段展开出的主机地址）。
/// 上限用于挡住把 `discover_subnets` 误配成 /16 之类后一次喷出几万个包的情况：
/// 512 大约够 2 个 /24 网段（其余是广播地址）。
pub const MAX_DISCOVER_TARGETS: usize = 512;

/// 配置网段允许的前缀范围：≥/24（一段最多 254 个主机），≤/30（再小就没有可用主机）。
const MIN_SCAN_PREFIX: u8 = 24;
const MAX_SCAN_PREFIX: u8 = 30;

/// 发现时等待回复的时间。设备 RTT 实测 ~2ms，1.5s 足够覆盖最慢的一条路径。
const DISCOVER_TIMEOUT_MS: u64 = 1500;

/// 一次“发现”的展开结果。
#[derive(Debug, Default)]
pub struct DiscoverPlan {
    /// 要发 Query 的单播目标（广播地址也在其中，`query_many` 会开 SO_BROADCAST）。
    pub targets: Vec<SocketAddr>,
    /// 实际展开扫描的网段（人类可读，用于日志/提示）。
    pub subnets: Vec<String>,
    /// 被跳过的项及原因（人类可读，用于日志/提示）。
    pub notes: Vec<String>,
}

/// 展开一次“发现”的全部单播目标：
/// 1) 本机每个非 loopback 网卡的广播地址 —— 覆盖同子网设备（与原行为一致；广播不跨网段，
///    所以这里**不**逐 IP 展开本机 /24：广播已覆盖，逐 IP 只会白刷一轮 ARP）；
/// 2) `extra_subnets` 里显式配置的网段/单机地址 —— 逐 IP 单播，跨网段发现靠这条，
///    例如设备被单位 DHCP 分到 `10.11.1.138` 时填 `10.11.1.0/24`。
///
/// 广播之外一律走单播，所以只要 IP 路由可达，设备就能被发现。
pub fn discover_plan(extra_subnets: &[String]) -> Result<DiscoverPlan, String> {
    let mut plan = DiscoverPlan::default();
    let mut targets: BTreeSet<SocketAddr> = BTreeSet::new();
    let mut local_ips: HashSet<Ipv4Addr> = HashSet::new();
    let mut broadcasts: Vec<Ipv4Addr> = Vec::new();

    for iface in
        device_discovery::list_iface_broadcasts().map_err(|e| format!("枚举本机网卡失败: {e}"))?
    {
        local_ips.insert(iface.ip);
        broadcasts.push(iface.broadcast);
        targets.insert(SocketAddr::V4(SocketAddrV4::new(iface.broadcast, DEFAULT_CTRL_PORT)));
    }
    if !broadcasts.is_empty() {
        plan.notes.push(format!(
            "本机广播目标：{}",
            broadcasts.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(", ")
        ));
    }

    for raw in extra_subnets {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        match s.split_once('/') {
            // 裸 IPv4 ⇒ 视作单个目标（/32），方便直接钉住一台设备
            None => match s.parse::<Ipv4Addr>() {
                Ok(ip) => {
                    if !local_ips.contains(&ip) {
                        targets.insert(SocketAddr::V4(SocketAddrV4::new(ip, DEFAULT_CTRL_PORT)));
                    }
                    plan.subnets.push(format!("{ip}/32 [配置]"));
                }
                Err(_) => plan.notes.push(format!("跳过 {s:?}：既不是 CIDR 网段也不是 IPv4 地址")),
            },
            Some((addr, plen)) => {
                let ip = addr
                    .trim()
                    .parse::<Ipv4Addr>()
                    .map_err(|_| format!("discover_subnets 非法 IPv4: {addr:?}"))?;
                let prefix: u8 = plen
                    .trim()
                    .parse()
                    .map_err(|_| format!("discover_subnets 非法前缀长度: {plen:?}"))?;
                if !(MIN_SCAN_PREFIX..=MAX_SCAN_PREFIX).contains(&prefix) {
                    plan.notes.push(format!(
                        "跳过 {s:?}：只扫描 /{MIN_SCAN_PREFIX}../{MAX_SCAN_PREFIX} 的网段（一次最多 254 个目标）"
                    ));
                    continue;
                }
                let mask = prefix_mask(prefix);
                let net = Ipv4Addr::from(u32::from(ip) & mask);
                plan.subnets.push(format!("{net}/{prefix} [配置]"));
                for h in hosts_in(net, prefix) {
                    if !local_ips.contains(&h) {
                        targets.insert(SocketAddr::V4(SocketAddrV4::new(h, DEFAULT_CTRL_PORT)));
                    }
                }
            }
        }
    }

    if targets.len() > MAX_DISCOVER_TARGETS {
        return Err(format!(
            "发现目标过多（{} > {MAX_DISCOVER_TARGETS}）：请收窄 discover_subnets",
            targets.len()
        ));
    }
    plan.targets = targets.into_iter().collect();
    Ok(plan)
}

/// 向多个目标单播 `Query` 并收集应答地址。
///
/// 不复用 `syncdaq::ctrl_msg::send_cmd`：它把 socket 设成非阻塞后对每个目标
/// `send_to(...).expect(...)`，而扫一个**直连** /24 时 253 个目标各有各的下一跳，
/// 内核 ARP 未解析队列会被灌满，`send_to` 直接返回 EAGAIN 把一个后台线程 panic 掉。
/// 这里对 EAGAIN 做小睡重试、对个体失败只记录不 panic，并在发送间隙限速。
fn query_many(targets: &[SocketAddr], timeout: Duration) -> (Vec<SocketAddr>, Vec<String>) {
    let socket = match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(s) => s,
        Err(e) => return (Vec::new(), vec![format!("绑定本地 UDP 端口失败: {e}")]),
    };
    let _ = socket.set_broadcast(true);
    let _ = socket.set_read_timeout(Some(timeout));
    let _ = socket.set_nonblocking(true);

    let mut pending: BTreeSet<u32> = BTreeSet::new();
    let mut send_failed = 0usize;
    for (i, target) in targets.iter().enumerate() {
        let msg_id = i as u32 + 1;
        let cmd = CtrlMsg::Query { msg_id };
        let mut cursor = Cursor::new(Vec::new());
        if cmd.write(&mut cursor).is_err() {
            send_failed += 1;
            continue;
        }
        let buf = cursor.into_inner();

        let mut tries = 0;
        loop {
            match socket.send_to(&buf, target) {
                Ok(_) => {
                    pending.insert(msg_id);
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && tries < 10 => {
                    tries += 1;
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(_) => {
                    send_failed += 1;
                    break;
                }
            }
        }
        // 限速：每个包之间间隔 ~0.8ms（258 个目标约 0.25s）。
        // 一次性把 258 个 Query 突发出去时，路径上的风暴控制/设备侧协议栈实测会偶发丢掉
        // 那唯一一个真正需要的回复，扫完却报「本次应答 0 台」。
        std::thread::sleep(Duration::from_micros(800));
    }

    // 发送阶段结束后转为阻塞收（read_timeout 生效），收满或超时即止。
    let _ = socket.set_nonblocking(false);
    let mut buf = vec![0_u8; 9000];
    let mut replies: Vec<SocketAddr> = Vec::new();
    let mut undecodable = 0usize;
    let mut stale = 0usize;
    let mut recv_err: Option<std::io::Error> = None;
    while !pending.is_empty() {
        let (len, from) = match socket.recv_from(&mut buf) {
            Ok(v) => v,
            // 超时（正常收工）或其它错误（异常，记下来）
            Err(e) => {
                if e.kind() != std::io::ErrorKind::WouldBlock && e.kind() != std::io::ErrorKind::TimedOut {
                    recv_err = Some(e);
                }
                break;
            }
        };
        let mut cursor = Cursor::new(&buf[..len]);
        // 非本协议的杂包/空包直接忽略——宁可漏一个目标，也不能 panic
        let Ok(msg) = CtrlMsg::read(&mut cursor) else {
            undecodable += 1;
            continue;
        };
        if pending.remove(&msg.get_msg_id()) {
            if parse_status(&from, &msg).is_some() {
                replies.push(from);
            }
        } else {
            stale += 1;
        }
    }
    replies.sort();
    replies.dedup();

    let mut notes = Vec::new();
    if let Some(e) = recv_err {
        notes.push(format!("收包中断: {e}"));
    }
    if undecodable > 0 || stale > 0 {
        notes.push(format!(
            "收到 {undecodable} 个无法解码的包、{stale} 个 msg_id 对不上的包（已忽略）"
        ));
    }
    if send_failed > 0 {
        notes.push(format!(
            "{send_failed} 个目标未能发出（内核发送队列/ARP 队列满，已跳过）"
        ));
    }
    (replies, notes)
}

/// 执行一次发现：把广播目标与扫描目标一起单播 Query，返回回复的设备地址与过程说明。
///
/// 回复按地址去重排序；`notes` 供调用方打日志/提示（哪些网段跳过了、扫了多少目标）。
pub fn discover(extra_subnets: &[String]) -> Result<(Vec<SocketAddr>, Vec<String>), String> {
    let plan = discover_plan(extra_subnets)?;
    let mut notes = plan.notes;
    if plan.targets.is_empty() {
        notes.push("没有可扫描的目标（本机无可用网卡且 discover_subnets 为空）".to_string());
        return Ok((Vec::new(), notes));
    }
    let (addrs, send_notes) = query_many(&plan.targets, Duration::from_millis(DISCOVER_TIMEOUT_MS));
    notes.extend(send_notes);
    notes.push(format!(
        "扫描 {} 个目标（{}），本次应答 {} 台",
        plan.targets.len(),
        if plan.subnets.is_empty() { "-".to_string() } else { plan.subnets.join(", ") },
        addrs.len()
    ));
    Ok((addrs, notes))
}

/// 网段内可用主机地址（去掉网络号与广播地址）。
fn hosts_in(net: Ipv4Addr, prefix: u8) -> Vec<Ipv4Addr> {
    let mask = prefix_mask(prefix);
    let net_u = u32::from(net) & mask;
    let bcast = net_u | !mask;
    ((net_u + 1)..bcast).map(Ipv4Addr::from).collect()
}

fn prefix_mask(prefix: u8) -> u32 {
    if prefix == 0 { 0 } else { u32::MAX << (32 - prefix as u32) }
}

/// 查询指定设备的控制信息与各 port 的收包地址（**不经广播**）。
///
/// `syncdaq::device_discovery::get_device_info` 内部靠 `enumerate_device_addr()`（二层广播）
/// 先把设备找出来，设备一旦被 DHCP 分到别的网段就永远返回 `None`，采集会直接报
/// "no device info for this ip" —— 连设备的 XGbe 配置都没机会读到。
/// 这里改成直接单播 `XGbeCfgQuery` 到 `ip:3000`：控制面既然路由可达就够了。
///
/// payload 地址的判据是**该 port 的 `dst_ip` 是否属于本机某张网卡**（这正是收包时要 bind 的地址）；
/// 不校验 `dst_mac`：跨网段时那是网关的 MAC，同网段时才是本机网卡 MAC，两种情况都不影响 bind。
pub fn get_device_info(ip: Ipv4Addr) -> Option<DeviceInfo> {
    let target = SocketAddr::new(std::net::IpAddr::V4(ip), DEFAULT_CTRL_PORT);
    let cfg = xgbe_cfg(target, 0, DISCOVER_TIMEOUT_MS).ok()?;

    let local_ips = local_ipv4_addrs();
    let mut foreign = 0usize;
    let payload_addr = cfg
        .iter()
        .map(|c| {
            let dst = Ipv4Addr::from(c.dst_ip);
            if dst.is_unspecified() || !local_ips.contains(&dst) {
                foreign += 1;
                return None;
            }
            Some(SocketAddrV4::new(dst, c.dst_port))
        })
        .collect();
    if foreign > 0 {
        tracing::warn!(
            "{ip}: {foreign} 个 XGbe port 的 dst_ip 不属于本机网卡（本机地址: {:?}），这些 port 不会收包",
            local_ips
        );
    }
    Some(DeviceInfo { ctrl_addr: target, payload_addr })
}

/// 本机当前所有 IPv4 地址（非 loopback）。收包前用它判断设备的数据目标是不是发给本机的。
pub fn local_ipv4_addrs() -> HashSet<Ipv4Addr> {
    device_discovery::list_iface_broadcasts()
        .map(|v| v.into_iter().map(|i| i.ip).collect())
        .unwrap_or_default()
}

pub fn send(
    cmd: CtrlMsg,
    target: SocketAddr,
    _local_port: u16,
    timeout_ms: u64,
    debug: u32,
) -> CmdReplySummary {
    // 使用临时(ephemeral)本地端口发送：避免同一固定端口被多路顺序复用，
    // 前一次查询的迟到回复落进下一次 socket 时触发 syncdaq::send_cmd 内的 assert 崩溃。
    send_cmd(
        cmd,
        &[target],
        (Ipv4Addr::UNSPECIFIED, 0),
        Some(Duration::from_millis(timeout_ms)),
        debug,
    )
}

pub fn parse_target(s: &str) -> Result<SocketAddr, String> {
    s.parse::<SocketAddr>().map_err(|e| format!("invalid addr {s:?}: {e}"))
}

#[derive(Serialize, Clone)]
pub struct ReplyView {
    pub addr: String,
    pub data: serde_json::Value,
}

#[derive(Serialize, Clone)]
pub struct SummaryView {
    pub no_reply: Vec<String>,
    pub invalid: Vec<ReplyView>,
    pub normal: Vec<ReplyView>,
}

impl From<CmdReplySummary> for SummaryView {
    fn from(s: CmdReplySummary) -> Self {
        SummaryView {
            no_reply: s
                .no_reply
                .iter()
                .map(|(a, _)| format!("{a:?}"))
                .collect(),
            invalid: s
                .invalid_reply
                .iter()
                .map(|(a, m)| ReplyView {
                    addr: a.to_string(),
                    data: serde_json::to_value(m).unwrap_or_default(),
                })
                .collect(),
            normal: s
                .normal_reply
                .iter()
                .map(|(a, m)| ReplyView {
                    addr: a.to_string(),
                    data: serde_json::to_value(m).unwrap_or_default(),
                })
                .collect(),
        }
    }
}

#[derive(Serialize, Clone)]
pub struct HealthView {
    /// sw 编译时间（本地时区字符串，解码自 QueryReply._z）
    pub sw_build_time: String,
    pub rfdc_restart_cnt: u32,
    pub temperature: f32,
    pub nports: u32,
    /// boot 模式原始值
    pub boot_mode: u32,
    /// boot 模式名（JTAG/QSPI32/SD0…）
    pub boot_mode_name: String,
    /// 开机运行时长（秒）
    pub uptime_s: u32,
    /// 当前 fifo 占用数（fifo_full_cnt 高 16 位）
    pub fifo_len: u32,
    /// 历史最大 fifo 占用数（fifo_full_cnt 低 16 位，仅重启复位）
    pub fifo_max: u32,
    pub smp_rate: u32,
    /// 采样位宽（smp_bits）
    pub smp_bits: u32,
    /// 数据格式：1=IQ，其它=real（iq_mode）
    pub iq_mode: u32,
    pub fan_pulse_cnt: u32,
    pub over_voltage_state: u32,
    pub over_range_state: u32,
    pub pkt_cnt1: Vec<u64>,
    pub pkt_cnt2: Vec<u64>,
    pub axi_frame_cnt1: Vec<u64>,
    pub axi_frame_cnt2: Vec<u64>,
}

#[derive(Serialize, Clone)]
pub struct StatusSnapshot {
    pub addr: String,
    pub fm_year: u32,
    pub fm_month: u32,
    pub fm_day: u32,
    pub fm_hour: u32,
    pub fm_minute: u32,
    pub fm_second: u32,
    pub tick_cnt1: u32,
    pub tick_cnt2: u32,
    pub trans_state: u32,
    pub locked: u32,
    pub sd_lock_ok: bool,
    pub tick_ok: bool,
    pub health: HealthView,
    pub ts: i64,
}

pub fn parse_status(addr: &SocketAddr, msg: &CtrlMsg) -> Option<StatusSnapshot> {
    if let CtrlMsg::QueryReply {
        msg_id: _,
        fm_ver,
        tick_cnt1,
        tick_cnt2,
        trans_state,
        locked,
        _z,
        rfdc_restart_cnt,
        temperature,
        nports,
        boot_mode,
        uptime_s,
        fifo_full_cnt,
        over_voltage_state,
        over_range_state,
        smp_rate,
        smp_bits,
        iq_mode,
        fan_pulse_cnt,
        pkt_cnt1,
        axi_frame_cnt1,
        pkt_cnt2,
        axi_frame_cnt2,
    } = msg
    {
        let (fm_year, fm_month, fm_day, fm_hour, fm_minute, fm_second) = unpack_fm_ver(*fm_ver);
        Some(StatusSnapshot {
            addr: addr.to_string(),
            fm_year,
            fm_month,
            fm_day,
            fm_hour,
            fm_minute,
            fm_second,
            tick_cnt1: *tick_cnt1,
            tick_cnt2: *tick_cnt2,
            trans_state: *trans_state,
            locked: *locked,
            sd_lock_ok: locked & 0x00_00_00_0f == 0x0f,
            tick_ok: *tick_cnt2 - *tick_cnt1 == 10_000_000,
            health: HealthView {
                sw_build_time: sw_build_time_local(*_z),
                rfdc_restart_cnt: *rfdc_restart_cnt,
                temperature: *temperature,
                nports: *nports,
                boot_mode: *boot_mode,
                boot_mode_name: boot_mode_name(*boot_mode).to_string(),
                uptime_s: *uptime_s,
                fifo_len: *fifo_full_cnt >> 16,
                fifo_max: *fifo_full_cnt & 0xffff,
                smp_rate: *smp_rate,
                smp_bits: *smp_bits,
                iq_mode: *iq_mode,
                fan_pulse_cnt: *fan_pulse_cnt,
                over_voltage_state: *over_voltage_state,
                over_range_state: *over_range_state,
                pkt_cnt1: pkt_cnt1.clone(),
                pkt_cnt2: pkt_cnt2.clone(),
                axi_frame_cnt1: axi_frame_cnt1.clone(),
                axi_frame_cnt2: axi_frame_cnt2.clone(),
            },
            ts: chrono::Utc::now().timestamp_millis(),
        })
    } else {
        None
    }
}

/// boot 模式名（与 syncdaq 的 boot_mode_name 一致）。
fn boot_mode_name(m: u32) -> &'static str {
    match m & 0xF {
        0 => "JTAG",
        1 => "QSPI24",
        2 => "QSPI32",
        3 => "SD0(1.8V)",
        4 => "NAND",
        5 => "SD1",
        6 => "MMC",
        7 => "USB",
        8 => "EMMC",
        14 => "SD0(3.0V)",
        _ => "?",
    }
}

/// 解码 sw 编译时间位域（UTC），转本地时区显示。位域同 fm_ver。
fn sw_build_time_local(z: u32) -> String {
    let day = (z >> 27) & 0x1F;
    let month = (z >> 23) & 0x0F;
    let year = 2000 + ((z >> 17) & 0x3F);
    let hour = (z >> 12) & 0x1F;
    let minute = (z >> 6) & 0x3F;
    let second = z & 0x3F;
    use chrono::{Local, TimeZone, Utc};
    match Utc.with_ymd_and_hms(year as i32, month, day, hour, minute, second).single() {
        Some(dt) => dt.with_timezone(&Local).format("%Y-%m-%d %H:%M:%S").to_string(),
        None => format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02} UTC"),
    }
}

fn unpack_fm_ver(fm_ver: u32) -> (u32, u32, u32, u32, u32, u32) {
    let day = (fm_ver >> 27) & 0x1f;
    let month = (fm_ver >> 23) & 0x0f;
    let year = 2000 + ((fm_ver >> 17) & 0x3f);
    let hour = (fm_ver >> 12) & 0x1f;
    let minute = (fm_ver >> 6) & 0x3f;
    let second = fm_ver & 0x3f;
    (year, month, day, hour, minute, second)
}

pub fn cmd_query() -> CtrlMsg {
    CtrlMsg::Query { msg_id: 0 }
}
pub fn cmd_sync() -> CtrlMsg {
    CtrlMsg::Sync { msg_id: 0 }
}

/// 返回命令的可读标签，用于日志打印初始化等步骤细节。
pub fn cmd_label(cmd: &CtrlMsg) -> String {
    match cmd {
        CtrlMsg::SetClk { clk_src, pps_src, .. } => format!("SetClk(clk_src={clk_src}, pps_src={pps_src})"),
        CtrlMsg::Sync { .. } => "Sync".to_string(),
        CtrlMsg::XGbeCfgSingle { port_id, .. } => format!("XGbeCfgSingle(port {port_id})"),
        CtrlMsg::MixerSet { freq, sync, .. } => format!("MixerSet(freq={freq:?}, sync={sync})"),
        CtrlMsg::StreamStart { .. } => "StreamStart".to_string(),
        CtrlMsg::StreamStop { .. } => "StreamStop".to_string(),
        CtrlMsg::Query { .. } => "Query".to_string(),
        CtrlMsg::Reboot { .. } => "Reboot".to_string(),
        CtrlMsg::GetDSA { port_id, .. } => format!("GetDSA(port {port_id})"),
        CtrlMsg::SetDSA { port_id, .. } => format!("SetDSA(port {port_id})"),
        CtrlMsg::QsfpInfo { .. } => "QsfpInfo".to_string(),
        CtrlMsg::XGbeCfgQuery { .. } => "XGbeCfgQuery".to_string(),
        _ => "CtrlMsg".to_string(),
    }
}pub fn cmd_stream_start() -> CtrlMsg {
    CtrlMsg::StreamStart { msg_id: 0 }
}
pub fn cmd_stream_stop() -> CtrlMsg {
    CtrlMsg::StreamStop { msg_id: 0 }
}
pub fn cmd_reboot() -> CtrlMsg {
    CtrlMsg::Reboot { msg_id: 0, mode: 0 }
}
pub fn cmd_clr_ov() -> CtrlMsg {
    CtrlMsg::ClrOv { msg_id: 0 }
}
pub fn cmd_mixer(freq_mhz: f64, sync: u32) -> CtrlMsg {
    CtrlMsg::MixerSet {
        msg_id: 0,
        nports: 8,
        // 一律送**正**的本振频率：-Q 由 FPGA 侧的 reset_mixer()
        // （各工程 sw/daq_sys/src/rfdc.c）统一取负一次实现。
        // PL 的 "Invert Q Output" 在 Vivado/Vitis 2025.2 上会被
        // XRFdc_SetFineMixer() 覆盖掉，不能依赖。
        freq: vec![freq_mhz; 8],
        phase: vec![0.0; 8],
        sync,
    }
}
pub fn cmd_get_dsa(port_id: u32) -> CtrlMsg {
    CtrlMsg::GetDSA { msg_id: 0, port_id }
}
pub fn cmd_set_dsa(port_id: u32, dsa_value: f32) -> CtrlMsg {
    CtrlMsg::SetDSA { msg_id: 0, port_id, dsa_value }
}
pub fn cmd_set_clk(clk_src: u32, pps_src: u32) -> CtrlMsg {
    CtrlMsg::SetClk { msg_id: 0, clk_src, pps_src }
}
pub fn cmd_qsfp_info() -> CtrlMsg {
    CtrlMsg::QsfpInfo { msg_id: 0 }
}
/// 构造 QsfpSet：cdr_ctrl 仅第一个元素有意义（8bit，bit0-3=rx ch1-4、bit4-7=tx ch1-4）；
/// eq_ctrl [i8;4]、adapt_eq [u8;4] 为 4 通道各一个值；restl/modsell/lpmode 取参考默认（1/0/0）。
pub fn cmd_qsfp_set(cdr_ctrl: u8, eq_ctrl: [i8; 4], adapt_eq: [u8; 4]) -> CtrlMsg {
    CtrlMsg::QsfpSet {
        msg_id: 0,
        restl: 1,
        modsell: 0,
        lpmode: 0,
        cdr_ctrl: [cdr_ctrl, 0, 0, 0],
        eq_ctrl,
        adapt_eq,
    }
}
pub fn cmd_xgbe_query() -> CtrlMsg {
    CtrlMsg::XGbeCfgQuery { msg_id: 0 }
}

/// 从 `XGbeCfgSingle` 的成员构造命令。
pub fn cmd_xgbe_cfg_single(
    port_id: u32,
    cfg: syncdaq::ctrl_msg::XGbeCfg,
) -> CtrlMsg {
    CtrlMsg::XGbeCfgSingle { msg_id: 0, port_id, cfg }
}

/// 查询设备当前的 XGbe 配置（每 port 的 dst_mac / src_mac / ip / port）。
/// 设备规则：若某 port 的 `dst_mac` 全 0，则该 port 被禁用、设备不会发这一路数据。
pub fn xgbe_cfg(device: SocketAddr, local_port: u16, timeout_ms: u64) -> Result<Vec<syncdaq::ctrl_msg::XGbeCfg>, String> {
    let s = send(cmd_xgbe_query(), device, local_port, timeout_ms, 0);
    s.normal_reply
        .iter()
        .find_map(|(_, m)| match m {
            CtrlMsg::XGbeCfgQueryReply { cfg, .. } => Some(cfg.clone()),
            _ => None,
        })
        .ok_or_else(|| "XGbeCfgQuery 无回复".to_string())
}

/// 时钟源 → (clk_src, pps_src)。`gps`=板上 GPS 模块 10M+PPS；`ext_clk`=外接同轴 10M+PPS。
pub fn clock_source_cfg(src: &str) -> (u32, u32) {
    match src.trim().to_ascii_lowercase().as_str() {
        "ext_clk" | "ext" | "external" => (2, 1),
        _ => (1, 0), // gps 默认
    }
}

/// 用 Rust 结构体直接构造“初始化”命令序列（不再依赖文本 YAML）。
///
/// XGbeCfg 以「设备当前配置」为基准重放（幂等、安全）：`dst_mac`/`src_mac`/ip/port
/// 全部取自设备当前值，避免用陈旧样例里的 `dst_mac` 覆盖掉与本机匹配的现网配置。
/// 另附 `SetClk`（按 clock_source 选 gps/ext_clk）、`Sync`、`MixerSet`。
pub fn default_init_commands(
    device: SocketAddr,
    local_port: u16,
    timeout_ms: u64,
    mixer_freq_mhz: f64,
    clock_source: &str,
) -> Result<Vec<CtrlMsg>, String> {
    let (clk_src, pps_src) = clock_source_cfg(clock_source);
    let cfg = xgbe_cfg(device, local_port, timeout_ms)?;
    let mut cmds = Vec::with_capacity(cfg.len() + 3);
    cmds.push(CtrlMsg::SetClk { msg_id: 0, clk_src, pps_src });
    cmds.push(CtrlMsg::Sync { msg_id: 0 });
    for (i, c) in cfg.iter().enumerate() {
        cmds.push(CtrlMsg::XGbeCfgSingle { msg_id: 0, port_id: i as u32, cfg: c.clone() });
    }
    cmds.push(CtrlMsg::MixerSet {
        msg_id: 0,
        nports: 8,
        // 同 cmd_mixer()：送正值，-Q 由 FPGA 侧 reset_mixer() 取负一次完成。
        freq: vec![mixer_freq_mhz; 8],
        phase: vec![0.0; 8],
        sync: 1,
    });
    Ok(cmds)
}
