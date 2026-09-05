use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex, RwLock,
        atomic::Ordering,
    },
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        Path, State, WebSocketUpgrade, Query,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use futures_util::{SinkExt as _, StreamExt as _};

use askama::Template;

use syncdaq::ctrl_msg::{CtrlMsg, XGbeCfg};

mod capture;
mod config;
mod control;
mod settings;

use capture::{CaptureHandle, CaptureResult, PortProgress};
use config::Config;
use control::{StatusSnapshot, SummaryView};
use settings::{DeviceSettings, DsaSetting, QsfpSetting, XgbePortSetting};

// ---------------------------------------------------------------- state

#[derive(Clone, Serialize)]
struct DeviceEntry {
    ctrl_addr: String,
    ip: String,
    port: u16,
}

#[derive(Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsMsg {
    Status {
        payload: StatusSnapshot,
    },
    CaptureProgress {
        capture_id: String,
        phase: String,
        progress: Vec<PortProgress>,
    },
    CaptureResult {
        capture_id: String,
        result: CaptureResult,
    },
    CaptureError {
        capture_id: String,
        error: String,
    },
    InitStep {
        init_id: String,
        index: usize,
        name: String,
        reply: SummaryView,
    },
    InitDone {
        init_id: String,
        steps: usize,
    },
    InitError {
        init_id: String,
        error: String,
    },
}

struct AppState {
    config: RwLock<Config>,
    devices: RwLock<Vec<DeviceEntry>>,
    status: RwLock<Option<StatusSnapshot>>,
    ws_tx: broadcast::Sender<WsMsg>,
    captures: Mutex<std::collections::HashMap<String, Arc<CaptureHandle>>>,
    /// 串行化所有控制面发送，避免多个 `send_cmd` 同时绑定同一本地控制端口而 panic。
    ctrl_lock: Arc<std::sync::Mutex<()>>,
    /// 设备状态（XGbe/DSA/QSFP），持久化到后端文件。
    settings: RwLock<DeviceSettings>,
}

type SharedState = Arc<AppState>;

// ---------------------------------------------------------------- errors

struct ApiError {
    status: StatusCode,
    msg: String,
}

impl ApiError {
    fn bad(msg: impl Into<String>) -> Self {
        ApiError { status: StatusCode::BAD_REQUEST, msg: msg.into() }
    }
    fn internal(msg: impl Into<String>) -> Self {
        ApiError { status: StatusCode::INTERNAL_SERVER_ERROR, msg: msg.into() }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(serde_json::json!({ "error": self.msg }))).into_response()
    }
}

fn selected_addr(cfg: &Config) -> Result<SocketAddr, ApiError> {
    match &cfg.selected_device {
        Some(s) => control::parse_target(s).map_err(ApiError::bad),
        None => Err(ApiError::bad("还未选择设备：请先在页面『发现设备』并选中一个")),
    }
}

// ---------------------------------------------------------------- cmd helpers

async fn run_cmd(st: &SharedState, cmd: CtrlMsg) -> Result<Json<SummaryView>, ApiError> {
    let (target, local_port, timeout) = {
        let cfg = st.config.read().unwrap();
        (selected_addr(&cfg)?, cfg.local_ctrl_port, cfg.timeout_ms)
    };
    let out = {
        let lock = st.ctrl_lock.clone();
        tokio::task::spawn_blocking(move || {
            let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
            control::send(cmd, target, local_port, timeout, 0)
        })
        .await
        .map_err(|e| ApiError::internal(format!("spawn_blocking: {e}")))?
    };
    Ok(Json(out.into()))
}

// ---------------------------------------------------------------- requests DTOs

#[derive(Deserialize)]
struct MixerReq {
    freq_mhz: f64,
    sync: u32,
}
#[derive(Deserialize)]
struct DsaSetReq {
    port_id: u32,
    dsa_value: f32,
}
#[derive(Deserialize)]
struct DsaGetReq {
    port_id: u32,
}
#[derive(Deserialize)]
struct ClkReq {
    clk_src: u32,
    #[serde(default)]
    pps_src: u32,
}
#[derive(Deserialize)]
struct SelectReq {
    addr: String,
}
#[derive(Deserialize)]
struct CaptureRequest {
    device_ip: Option<String>,
    frames_per_port: Option<usize>,
    fft_size: Option<usize>,
    window: Option<String>,
    timeout_ms: Option<u64>,
}

// ---------------------------------------------------------------- page

#[derive(askama::Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    poll_interval_ms: u64,
    default_frames_per_port: usize,
    default_fft_size: usize,
    default_addr: String,
}

async fn index(st: State<SharedState>) -> impl IntoResponse {
    let cfg = st.config.read().unwrap();
    let tpl = IndexTemplate {
        poll_interval_ms: cfg.poll_interval_ms,
        default_frames_per_port: cfg.capture.frames_per_port,
        default_fft_size: cfg.capture.fft_size,
        default_addr: cfg.selected_device.clone().unwrap_or_default(),
    };
    match tpl.render() {
        Ok(s) => Html(s).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("template error: {e}")).into_response(),
    }
}

// ---------------------------------------------------------------- handlers

async fn get_config(st: State<SharedState>) -> Json<Config> {
    Json(st.config.read().unwrap().clone())
}

/// 读取持久化的设备状态（XGbe/DSA/QSFP）。
async fn get_settings(st: State<SharedState>) -> Json<DeviceSettings> {
    Json(st.settings.read().unwrap().clone())
}

async fn put_config(st: State<SharedState>, Json(cfg): Json<Config>) -> Json<Config> {
    *st.config.write().unwrap() = cfg.clone();
    cfg.save();
    Json(cfg)
}

async fn discover(st: State<SharedState>) -> Result<Json<Vec<DeviceEntry>>, ApiError> {
    let lock = st.ctrl_lock.clone();
    let list = tokio::task::spawn_blocking(move || {
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        control::enumerate()
    })
    .await
    .map_err(|e| ApiError::internal(format!("discover task: {e}")))?
    .map_err(|e| ApiError::internal(format!("discover: {e}")))?;

    let mut seen: std::collections::HashSet<String> =
        st.devices.read().unwrap().iter().map(|d| d.ctrl_addr.clone()).collect();
    for addr in list {
        let ip = addr.ip().to_string();
        let port = addr.port();
        if seen.insert(addr.to_string()) {
            st.devices.write().unwrap().push(DeviceEntry { ctrl_addr: addr.to_string(), ip, port });
        }
    }
    Ok(Json(st.devices.read().unwrap().clone()))
}

async fn get_devices(st: State<SharedState>) -> Json<Vec<DeviceEntry>> {
    Json(st.devices.read().unwrap().clone())
}

async fn select_device(st: State<SharedState>, Json(req): Json<SelectReq>) -> Result<Json<serde_json::Value>, ApiError> {
    let _ = control::parse_target(&req.addr).map_err(ApiError::bad)?;
    {
        let mut cfg = st.config.write().unwrap();
        cfg.selected_device = Some(req.addr.clone());
        cfg.save();
    }
    Ok(Json(serde_json::json!({ "selected_device": req.addr })))
}

async fn do_init(st: State<SharedState>) -> Result<Json<serde_json::Value>, ApiError> {
    let (target, local_port, timeout, clock_source, freq_mhz) = {
        let cfg = st.config.read().unwrap();
        (
            selected_addr(&cfg)?,
            cfg.local_ctrl_port,
            cfg.timeout_ms,
            cfg.clock_source.clone(),
            // 初始化使用持久化的本振（与频谱 x 轴、混频面板联动）
            cfg.lo_mhz,
        )
    };
    let init_id = format!("init-{}", chrono::Utc::now().timestamp_millis());
    let init_id_task = init_id.clone();
    let st_arc = st.0.clone();
    let ws = st_arc.ws_tx.clone();
    let lock = st_arc.ctrl_lock.clone();
    let cmd_timeout = timeout.max(5000);

    tokio::spawn(async move {
        // 1) 生成初始化命令序列（含 Xgbe 探测，持锁）
        let l1 = lock.clone();
        let cmds = tokio::task::spawn_blocking(move || {
            let _g = l1.lock().unwrap_or_else(|e| e.into_inner());
            control::default_init_commands(target, local_port, timeout, freq_mhz, clock_source.as_str())
        })
        .await
        .ok()
        .and_then(|r| r.ok());
        let cmds = match cmds {
            Some(c) => c,
            None => {
                let _ = ws.send(WsMsg::InitError { init_id: init_id_task.clone(), error: "生成 init 命令失败".into() });
                return;
            }
        };

        // 2) 逐个下发，每完成一条就经 WS 推送到日志
        let mut steps = 0usize;
        for (idx, cmd) in cmds.into_iter().enumerate() {
            let name = control::cmd_label(&cmd);
            let li = lock.clone();
            let reply = tokio::task::spawn_blocking(move || {
                let _g = li.lock().unwrap_or_else(|e| e.into_inner());
                control::send(cmd, target, local_port, cmd_timeout, 0)
            })
            .await
            .ok();
            if let Some(r) = reply {
                let _ = ws.send(WsMsg::InitStep {
                    init_id: init_id_task.clone(),
                    index: idx,
                    name,
                    reply: SummaryView::from(r),
                });
                steps += 1;
            }
            // 命令间小延迟，便于状态反馈；若希望更快可调小/置 0
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let _ = ws.send(WsMsg::InitDone { init_id: init_id_task.clone(), steps });
    });

    Ok(Json(serde_json::json!({ "init_id": init_id })))
}

async fn do_status(st: State<SharedState>) -> Result<Json<StatusSnapshot>, ApiError> {
    let (target, local_port, timeout) = {
        let cfg = st.config.read().unwrap();
        (selected_addr(&cfg)?, cfg.local_ctrl_port, cfg.timeout_ms)
    };
    let lock = st.ctrl_lock.clone();
    let snap = tokio::task::spawn_blocking(move || {
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        let s = control::send(control::cmd_query(), target, local_port, timeout, 0);
        s.normal_reply.iter().find_map(|(a, m)| control::parse_status(a, m))
    })
    .await
    .map_err(|e| ApiError::internal(format!("status: {e}")))?
    .ok_or_else(|| ApiError::bad("设备未回复 Query"))?;
    *st.status.write().unwrap() = Some(snap.clone());
    let _ = st.ws_tx.send(WsMsg::Status { payload: snap.clone() });
    Ok(Json(snap))
}

async fn cmd_sync(st: State<SharedState>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_sync()).await
}
async fn cmd_stream_start(st: State<SharedState>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_stream_start()).await
}
async fn cmd_stream_stop(st: State<SharedState>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_stream_stop()).await
}
async fn cmd_reboot(st: State<SharedState>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_reboot()).await
}
async fn cmd_clrov(st: State<SharedState>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_clr_ov()).await
}
async fn cmd_qsfp(st: State<SharedState>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_qsfp_info()).await
}

#[derive(Deserialize)]
struct QsfpSetReq {
    cdr_ctrl: u8,
    eq_ctrl: [i8; 4],
    adapt_eq: [u8; 4],
}

/// 结构化 QSFP 信息：查询 QsfpInfo 并整理成便于前端展示的视图。
async fn get_qsfp(st: State<SharedState>) -> Result<Json<serde_json::Value>, ApiError> {
    let (target, local_port, timeout) = {
        let cfg = st.config.read().unwrap();
        (selected_addr(&cfg)?, cfg.local_ctrl_port, cfg.timeout_ms)
    };
    let lock = st.ctrl_lock.clone();
    let view = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, ApiError> {
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        let s = control::send(control::cmd_qsfp_info(), target, local_port, timeout, 0);
        s.normal_reply
            .iter()
            .find_map(|(_, m)| match m {
                CtrlMsg::QsfpInfoReply {
                    restl,
                    modsell,
                    lpmode,
                    modpresent,
                    temperature,
                    vcc,
                    tx_bias,
                    rx_power,
                    tx_power,
                    los_lol,
                    vcc_temp_alarm,
                    cdr_ctrl,
                    eq_ctrl,
                    adapt_eq,
                    ..
                } => Some(serde_json::json!({
                    "modpresent": *modpresent,
                    "lpmode": *lpmode,
                    "restl": *restl,
                    "modsell": *modsell,
                    "temperature": *temperature,
                    "vcc": *vcc,
                    "tx_bias": tx_bias,
                    "rx_power": rx_power,
                    "tx_power": tx_power,
                    "los_lol": los_lol,
                    "alarm": vcc_temp_alarm,
                    "cdr_ctrl": cdr_ctrl,
                    "cdr_hex": format!("0x{:02x}", cdr_ctrl[0]),
                    "eq_ctrl": eq_ctrl,
                    "adapt_eq": adapt_eq,
                })),
                _ => None,
            })
            .ok_or_else(|| ApiError::bad("QSFP 无回复"))
    })
    .await
    .map_err(|e| ApiError::internal(format!("qsfp: {e}")))??;
    Ok(Json(view))
}

/// 设置 QSFP：cdr_ctrl 以 8bit 十六进制数给出（bit0-3=rx ch1-4、bit4-7=tx ch1-4）；
/// eq_ctrl/adapt_eq 为 4 通道各一个值。
async fn set_qsfp(st: State<SharedState>, Json(req): Json<QsfpSetReq>) -> Result<Json<SummaryView>, ApiError> {
    let r = run_cmd(&st, control::cmd_qsfp_set(req.cdr_ctrl, req.eq_ctrl, req.adapt_eq)).await?;
    // 持久化本次 QSFP 设置
    {
        let mut set = st.settings.write().unwrap();
        set.qsfp = QsfpSetting { cdr_ctrl: req.cdr_ctrl as u32, eq_ctrl: req.eq_ctrl, adapt_eq: req.adapt_eq };
        set.save();
    }
    Ok(r)
}
async fn cmd_mixer(st: State<SharedState>, Json(req): Json<MixerReq>) -> Result<Json<SummaryView>, ApiError> {
    let r = run_cmd(&st, control::cmd_mixer(req.freq_mhz, req.sync)).await?;
    // 联动：本振 LO = 用户设置的 freq_mhz（负值由设备端取负 → LO显示取正）。
    {
        let mut cfg = st.config.write().unwrap();
        cfg.lo_mhz = req.freq_mhz;
        cfg.save();
    }
    Ok(r)
}
async fn cmd_dsa_set(st: State<SharedState>, Json(req): Json<DsaSetReq>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_set_dsa(req.port_id, req.dsa_value)).await
}
async fn cmd_dsa_get(st: State<SharedState>, Json(req): Json<DsaGetReq>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_get_dsa(req.port_id)).await
}
async fn cmd_clk(st: State<SharedState>, Json(req): Json<ClkReq>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_set_clk(req.clk_src, req.pps_src)).await
}
async fn cmd_xgbe_query(st: State<SharedState>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_xgbe_query()).await
}
async fn cmd_xgbe_single(st: State<SharedState>, Json(req): Json<XgbeSingleReq>) -> Result<Json<SummaryView>, ApiError> {
    run_cmd(&st, control::cmd_xgbe_cfg_single(req.port_id, req.cfg.into())).await
}

#[derive(Deserialize)]
struct XgbeSingleReq {
    port_id: u32,
    #[serde(flatten)]
    cfg: XgbeCfgDto,
}
#[derive(Deserialize)]
struct XgbeCfgDto {
    dst_mac: [u8; 6],
    src_mac: [u8; 6],
    dst_ip: [u8; 4],
    src_ip: [u8; 4],
    dst_port: u16,
    src_port: u16,
}
impl From<XgbeCfgDto> for XGbeCfg {
    fn from(d: XgbeCfgDto) -> Self {
        XGbeCfg {
            dst_mac: d.dst_mac,
            src_mac: d.src_mac,
            dst_ip: d.dst_ip,
            src_ip: d.src_ip,
            dst_port: d.dst_port,
            src_port: d.src_port,
        }
    }
}

// ---------------------------------------------------------------- XGbe config view (逐项配置)

#[derive(Serialize)]
struct XgbePortView {
    port: usize,
    dst_mac: String,
    src_mac: String,
    dst_ip: String,
    src_ip: String,
    dst_port: u16,
    src_port: u16,
}

#[derive(Serialize)]
struct XgbeConfigView {
    nports: usize,
    ports: Vec<XgbePortView>,
}

#[derive(Deserialize, Clone)]
struct XgbePortIn {
    port: u32,
    dst_mac: [u8; 6],
    src_mac: [u8; 6],
    dst_ip: [u8; 4],
    src_ip: [u8; 4],
    dst_port: u16,
    src_port: u16,
}

fn mac6_to_str(m: &[u8; 6]) -> String {
    m.iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(":")
}
fn ip4_to_str(i: &[u8; 4]) -> String {
    i.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(".")
}

fn xgbe_in_to_settings(ports: &[XgbePortIn]) -> Vec<XgbePortSetting> {
    ports
        .iter()
        .map(|p| XgbePortSetting {
            port: p.port,
            dst_mac: mac6_to_str(&p.dst_mac),
            src_mac: mac6_to_str(&p.src_mac),
            dst_ip: ip4_to_str(&p.dst_ip),
            src_ip: ip4_to_str(&p.src_ip),
            dst_port: p.dst_port,
            src_port: p.src_port,
        })
        .collect()
}

/// 查询当前 XGbe 配置，返回按 nports 的视图（MAC/IP 为字符串，便于前端回填）。
async fn get_xgbe_config(st: State<SharedState>) -> Result<Json<XgbeConfigView>, ApiError> {
    let (target, local_port, timeout) = {
        let cfg = st.config.read().unwrap();
        (selected_addr(&cfg)?, cfg.local_ctrl_port, cfg.timeout_ms)
    };
    let lock = st.ctrl_lock.clone();
    let cfg = tokio::task::spawn_blocking(move || {
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        control::xgbe_cfg(target, local_port, timeout).map_err(ApiError::bad)
    })
    .await
    .map_err(|e| ApiError::internal(format!("xgbe query: {e}")))??;

    let ports: Vec<XgbePortView> = cfg
        .into_iter()
        .enumerate()
        .map(|(i, c)| XgbePortView {
            port: i,
            dst_mac: mac6_to_str(&c.dst_mac),
            src_mac: mac6_to_str(&c.src_mac),
            dst_ip: ip4_to_str(&c.dst_ip),
            src_ip: ip4_to_str(&c.src_ip),
            dst_port: c.dst_port,
            src_port: c.src_port,
        })
        .collect();
    Ok(Json(XgbeConfigView { nports: ports.len(), ports }))
}

/// 批量下发 XGbeCfgSingle（逐项配置各端口 src/dst ip/mac/port）。
async fn set_xgbe_config(st: State<SharedState>, Json(ports): Json<Vec<XgbePortIn>>) -> Result<Json<serde_json::Value>, ApiError> {
    let (target, local_port, timeout) = {
        let cfg = st.config.read().unwrap();
        (selected_addr(&cfg)?, cfg.local_ctrl_port, cfg.timeout_ms)
    };
    let ports_for_settings = ports.clone();
    let lock = st.ctrl_lock.clone();
    let out = tokio::task::spawn_blocking(move || {
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut results = Vec::new();
        for p in ports {
            let cfg = XGbeCfg {
                dst_mac: p.dst_mac,
                src_mac: p.src_mac,
                dst_ip: p.dst_ip,
                src_ip: p.src_ip,
                dst_port: p.dst_port,
                src_port: p.src_port,
            };
            let s = control::send(control::cmd_xgbe_cfg_single(p.port, cfg), target, local_port, timeout, 0);
            results.push(SummaryView::from(s));
        }
        Ok::<_, ApiError>(results)
    })
    .await
    .map_err(|e| ApiError::internal(format!("xgbe set: {e}")))??;
    // 持久化本次 XGbe 每端口设置（设备状态）
    {
        let mut set = st.settings.write().unwrap();
        set.xgbe = xgbe_in_to_settings(&ports_for_settings);
        set.save();
    }
    Ok(Json(serde_json::json!({ "steps": out.len(), "results": out })))
}

// ---------------------------------------------------------------- DSA config (逐项增益)

#[derive(Deserialize)]
struct DsaQuery {
    nports: u32,
}

#[derive(Deserialize, Clone)]
struct DsaSetIn {
    port: u32,
    dsa_value: f32,
}
/// 逐 port 查询当前 DSA（GetDSA），返回 nports 条，no_reply 的 dsa_value 为 null。
async fn get_dsa_config(st: State<SharedState>, Query(q): Query<DsaQuery>) -> Result<Json<serde_json::Value>, ApiError> {
    let (target, local_port, timeout) = {
        let cfg = st.config.read().unwrap();
        (selected_addr(&cfg)?, cfg.local_ctrl_port, cfg.timeout_ms)
    };
    let n = q.nports.min(8);
    let lock = st.ctrl_lock.clone();
    let ports = tokio::task::spawn_blocking(move || {
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut v = Vec::with_capacity(n as usize);
        for p in 0..n {
            let s = control::send(control::cmd_get_dsa(p), target, local_port, timeout, 0);
            let val = s.normal_reply.iter().find_map(|(_, m)| match m {
                CtrlMsg::GetDSAReply { dsa_value, .. } => Some(*dsa_value),
                _ => None,
            });
            v.push(serde_json::json!({ "port": p, "dsa_value": val }));
        }
        Ok::<_, ApiError>(v)
    })
    .await
    .map_err(|e| ApiError::internal(format!("dsa query: {e}")))??;
    Ok(Json(serde_json::json!({ "ports": ports })))
}

/// 批量下发 SetDSA（逐项设置各 port 增益）。
async fn set_dsa_config(st: State<SharedState>, Json(ports): Json<Vec<DsaSetIn>>) -> Result<Json<serde_json::Value>, ApiError> {
    let (target, local_port, timeout) = {
        let cfg = st.config.read().unwrap();
        (selected_addr(&cfg)?, cfg.local_ctrl_port, cfg.timeout_ms)
    };
    let ports_for_settings = ports.clone();
    let lock = st.ctrl_lock.clone();
    let out = tokio::task::spawn_blocking(move || {
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut results = Vec::new();
        for p in ports {
            let s = control::send(control::cmd_set_dsa(p.port, p.dsa_value), target, local_port, timeout, 0);
            results.push(SummaryView::from(s));
        }
        Ok::<_, ApiError>(results)
    })
    .await
    .map_err(|e| ApiError::internal(format!("dsa set: {e}")))??;
    // 持久化本次 DSA 每端口设置（设备状态）
    {
        let mut set = st.settings.write().unwrap();
        set.dsa = ports_for_settings.iter().map(|p| DsaSetting { port: p.port, dsa_value: p.dsa_value }).collect();
        set.save();
    }
    Ok(Json(serde_json::json!({ "steps": out.len(), "results": out })))
}

async fn capture_start(st: State<SharedState>, Json(req): Json<CaptureRequest>) -> Result<Json<serde_json::Value>, ApiError> {
    // 确定目标 IP
    let cfg = st.config.read().unwrap();
    let ip = match &req.device_ip {
        Some(ip) => ip.clone(),
        None => {
            let sel = selected_addr(&cfg)?;
            sel.ip().to_string()
        }
    };
    let frames = req.frames_per_port.unwrap_or(cfg.capture.frames_per_port);
    let fft_size = req.fft_size.unwrap_or(cfg.capture.fft_size);
    let window = req.window.clone().unwrap_or_else(|| cfg.capture.window.clone());
    let timeout_ms = req.timeout_ms.unwrap_or(cfg.capture.timeout_ms);
    // 本振 LO 已与混频/初始化联动，频谱 x 轴始终用 config.lo_mhz
    let lo_mhz = cfg.lo_mhz;
    // 采集的控制通道用临时端口（避免复用固定 3001，防止迟到回复触发 syncdaq send_cmd 的 assert 崩溃）。
    let local_ctrl_port = 0u16;
    drop(cfg);

    let spec = capture::CaptureSpec::from_config(frames, fft_size, &window, timeout_ms, local_ctrl_port);
    let ip_v4: Ipv4Addr = ip.parse().map_err(|_| ApiError::bad(format!("invalid device ip {ip:?}")))?;

    let id = format!("cap-{}", chrono::Utc::now().timestamp_millis());
    let handle = Arc::new(capture::CaptureHandle::new(frames));

    let mut locks = st.captures.lock().unwrap_or_else(|e| e.into_inner());
    // 若已有在此 IP 上的采集在跑，先取消旧的一次
    for (k, h) in locks.iter() {
        if !h.done.load(Ordering::SeqCst) {
            h.abort.store(true, Ordering::SeqCst);
            let _ = k;
        }
    }
    locks.insert(id.clone(), handle.clone());
    drop(locks);

    let ws = st.ws_tx.clone();
    let run_h = handle.clone();
    let prog_h = handle.clone();
    let run_lock = st.ctrl_lock.clone();
    let id2 = id.clone();
    tokio::spawn(async move {
        let run = tokio::task::spawn_blocking(move || {
            capture::run_capture(ip_v4, &spec, &run_h, run_lock, lo_mhz);
        });
        // 进度转发
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if prog_h.done.load(Ordering::SeqCst) {
                break;
            }
            let phase = prog_h.phase.lock().unwrap().clone();
            let progress = prog_h.progress.lock().unwrap().clone();
            let _ = ws.send(WsMsg::CaptureProgress {
                capture_id: id2.clone(),
                phase,
                progress,
            });
        }
        let _ = run.await;
        let res = prog_h.result.lock().unwrap().clone();
        match res {
            Some(Ok(result)) => {
                let _ = ws.send(WsMsg::CaptureResult { capture_id: id2.clone(), result });
            }
            Some(Err(e)) => {
                let _ = ws.send(WsMsg::CaptureError { capture_id: id2.clone(), error: e });
            }
            None => {}
        }
    });

    Ok(Json(serde_json::json!({ "capture_id": id })))
}

async fn capture_list(st: State<SharedState>) -> Json<serde_json::Value> {
    let locks = st.captures.lock().unwrap_or_else(|e| e.into_inner());
    let arr: Vec<_> = locks
        .iter()
        .map(|(id, h)| {
            serde_json::json!({
                "capture_id": id,
                "done": h.done.load(Ordering::SeqCst),
                "aborted": h.abort.load(Ordering::SeqCst),
                "phase": h.phase.lock().unwrap().clone(),
            })
        })
        .collect();
    Json(serde_json::json!({ "captures": arr }))
}

async fn capture_cancel(st: State<SharedState>, Path(id): Path<String>) -> Result<Json<serde_json::Value>, ApiError> {
    let locks = st.captures.lock().unwrap_or_else(|e| e.into_inner());
    let h = locks.get(&id).ok_or_else(|| ApiError::bad(format!("no capture {id}")))?;
    h.abort.store(true, Ordering::SeqCst);
    Ok(Json(serde_json::json!({ "capture_id": id, "cancelled": true })))
}

async fn capture_result(st: State<SharedState>, Path(id): Path<String>) -> Result<Json<serde_json::Value>, ApiError> {
    let locks = st.captures.lock().unwrap_or_else(|e| e.into_inner());
    let h = locks.get(&id).ok_or_else(|| ApiError::bad(format!("no capture {id}")))?;
    if !h.done.load(Ordering::SeqCst) {
        return Ok(Json(serde_json::json!({ "capture_id": id, "done": false })));
    }
    let res = h.result.lock().unwrap().clone();
    match res {
        Some(Ok(spectra)) => Ok(Json(serde_json::json!({ "capture_id": id, "done": true, "result": spectra }))),
        Some(Err(e)) => Ok(Json(serde_json::json!({ "capture_id": id, "done": true, "error": e }))),
        None => Ok(Json(serde_json::json!({ "capture_id": id, "done": true }))),
    }
}

// ---------------------------------------------------------------- WS

async fn ws_handler(ws: WebSocketUpgrade, State(st): State<SharedState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| ws_conn(socket, st))
}

async fn ws_conn(socket: WebSocket, st: SharedState) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = st.ws_tx.subscribe();

    let initial: Option<StatusSnapshot> = st.status.read().unwrap().clone();
    if let Some(s) = initial {
        let _ = sender
            .send(Message::Text(serde_json::to_string(&WsMsg::Status { payload: s }).unwrap().into()))
            .await;
    }

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Ok(m) => {
                    let t = serde_json::to_string(&m).unwrap();
                    if sender.send(Message::Text(t.into())).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            },
            cmsg = receiver.next() => match cmsg {
                Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
                _ => break,
            },
        }
    }
}

// ---------------------------------------------------------------- status poller

async fn status_poller(st: SharedState) {
    loop {
        let (sel, local_port, timeout, interval) = {
            let cfg = st.config.read().unwrap();
            (cfg.selected_device.clone(), cfg.local_ctrl_port, cfg.timeout_ms, cfg.poll_interval_ms)
        };
        if let Some(sel) = sel {
            if let Ok(target) = control::parse_target(&sel) {
                let lock = st.ctrl_lock.clone();
                let snap = tokio::task::spawn_blocking(move || {
                    let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
                    let s = control::send(control::cmd_query(), target, local_port, timeout, 0);
                    s.normal_reply.iter().find_map(|(a, m)| control::parse_status(a, m))
                })
                .await
                .ok()
                .flatten();
                if let Some(s) = snap {
                    *st.status.write().unwrap() = Some(s.clone());
                    let _ = st.ws_tx.send(WsMsg::Status { payload: s });
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(interval)).await;
    }
}

// ---------------------------------------------------------------- main

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,syncdaq_web=debug".into()),
        )
        .init();

    let cfg = config::Config::load();
    let (ws_tx, _) = broadcast::channel::<WsMsg>(256);

    let st: SharedState = Arc::new(AppState {
        config: RwLock::new(cfg.clone()),
        devices: RwLock::new(Vec::new()),
        status: RwLock::new(None),
        ws_tx,
        captures: Mutex::new(std::collections::HashMap::new()),
        ctrl_lock: Arc::new(std::sync::Mutex::new(())),
        settings: RwLock::new(DeviceSettings::load()),
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(ws_handler))
        .route("/api/config", get(get_config).put(put_config))
        .route("/api/settings", get(get_settings))
        .route("/api/discover", post(discover))
        .route("/api/devices", get(get_devices))
        .route("/api/select", post(select_device))
        .route("/api/init", post(do_init))
        .route("/api/status", post(do_status))
        .route("/api/cmd/sync", post(cmd_sync))
        .route("/api/cmd/stream/start", post(cmd_stream_start))
        .route("/api/cmd/stream/stop", post(cmd_stream_stop))
        .route("/api/cmd/reboot", post(cmd_reboot))
        .route("/api/cmd/clrov", post(cmd_clrov))
        .route("/api/cmd/qsfp", post(cmd_qsfp))
        .route("/api/qsfp", get(get_qsfp).post(set_qsfp))
        .route("/api/cmd/mixer", post(cmd_mixer))
        .route("/api/cmd/dsa/set", post(cmd_dsa_set))
        .route("/api/cmd/dsa/get", post(cmd_dsa_get))
        .route("/api/cmd/clk", post(cmd_clk))
        .route("/api/cmd/xgbe/query", post(cmd_xgbe_query))
        .route("/api/cmd/xgbe/single", post(cmd_xgbe_single))
        .route("/api/xgbe/config", get(get_xgbe_config).post(set_xgbe_config))
        .route("/api/dsa/config", get(get_dsa_config).post(set_dsa_config))
        .route("/api/capture", get(capture_list).post(capture_start))
        .route("/api/capture/{id}/result", get(capture_result))
        .route("/api/capture/{id}/cancel", post(capture_cancel))
        .with_state(st.clone());

    let poll_st = st.clone();
    tokio::spawn(status_poller(poll_st));

    let addr = std::env::var("SYNCDAQ_WEB_ADDR").unwrap_or_else(|_| "127.0.0.1:3088".to_string());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("bind {addr}: {e}"));
    tracing::info!("syncdaq_web listening on http://{addr}");
    axum::serve(listener, app).await.unwrap();
}
