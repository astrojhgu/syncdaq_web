use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use serde::Serialize;

use syncdaq::ctrl_msg::{CmdReplySummary, CtrlMsg, Health, send_cmd};
use syncdaq::device_discovery::{self, DeviceInfo};

pub fn enumerate() -> std::io::Result<Vec<SocketAddr>> {
    device_discovery::enumerate_device_addr()
}

pub fn get_device_info(ip: Ipv4Addr) -> Option<DeviceInfo> {
    device_discovery::get_device_info(ip)
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
    pub rfdc_restart_cnt: u32,
    pub temperature: f32,
    pub nports: u32,
    pub fifo_full_cnt: u32,
    pub smp_rate: u32,
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
        health,
    } = msg
    {
        let (fm_year, fm_month, fm_day, fm_hour, fm_minute, fm_second) = unpack_fm_ver(*fm_ver);
        let Health::T510Health {
            rfdc_restart_cnt,
            temperature,
            nports,
            fifo_full_cnt,
            over_voltage_state,
            over_range_state,
            smp_rate,
            fan_pulse_cnt,
            pkt_cnt1,
            axi_frame_cnt1,
            pkt_cnt2,
            axi_frame_cnt2,
        } = health;
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
                rfdc_restart_cnt: *rfdc_restart_cnt,
                temperature: *temperature,
                nports: *nports,
                fifo_full_cnt: *fifo_full_cnt,
                smp_rate: *smp_rate,
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
    CtrlMsg::Reboot { msg_id: 0 }
}
pub fn cmd_clr_ov() -> CtrlMsg {
    CtrlMsg::ClrOv { msg_id: 0 }
}
pub fn cmd_mixer(freq_mhz: f64, sync: u32) -> CtrlMsg {
    CtrlMsg::MixerSet {
        msg_id: 0,
        nports: 8,
        freq: vec![-freq_mhz; 8],
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
        freq: vec![-mixer_freq_mhz; 8],
        phase: vec![0.0; 8],
        sync: 1,
    });
    Ok(cmds)
}
