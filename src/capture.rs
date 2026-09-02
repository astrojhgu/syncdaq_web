use std::{
    net::{Ipv4Addr, SocketAddrV4, UdpSocket},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use num_complex::Complex;
use rustfft::FftPlanner;

use syncdaq::{
    payload::{N_BYTE_PER_FRAME, Payload},
    sdr::SdrCtrl,
    utils::{as_mut_u8_slice, set_recv_buffer_size},
};

use crate::control::{get_device_info, parse_status, xgbe_cfg};

const PATCH_LEN: usize = N_BYTE_PER_FRAME / std::mem::size_of::<Complex<i16>>();
const RX_SOCKET_BUFFER: usize = 256 * 1024 * 1024;
const RECV_POLL_MS: u64 = 200;
const ARM_TIMEOUT_SECS: u64 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Window {
    Hann,
    Rect,
}

impl Window {
    pub fn from_name(s: &str) -> Window {
        match s.trim().to_ascii_lowercase().as_str() {
            "rect" | "none" | "boxcar" => Window::Rect,
            _ => Window::Hann,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CaptureSpec {
    pub frames_per_port: usize,
    pub fft_size: usize,
    pub window: Window,
    pub timeout_ms: u64,
    pub local_ctrl_port: u16,
}

impl CaptureSpec {
    pub fn from_config(
        frames: usize,
        fft_size: usize,
        window: &str,
        timeout_ms: u64,
        local_ctrl_port: u16,
    ) -> Self {
        CaptureSpec {
            frames_per_port: frames,
            fft_size,
            window: Window::from_name(window),
            timeout_ms,
            local_ctrl_port,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PortProgress {
    pub port: usize,
    pub received: usize,
    pub total: usize,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PortSpectrum {
    pub port: usize,
    pub samples_used: usize,
    pub blocks: usize,
    /// 全带宽谱（居中排列）：bins[0] 对应 -fs/2，bins[N/2] 对应 0(DC)，bins[N-1] 对应 +fs/2-Δ。
    /// 长度 = fft_size。
    pub bins: Vec<f32>,
    pub bin_width_hz: f64,
    pub smp_rate_mhz: f64,
    /// 本振频率（MHz），用于把 x 轴偏移为 RF = lo_mhz + 基带频率。
    pub lo_mhz: f64,
    pub duration_ms: f64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PortHistogram {
    pub port: usize,
    /// 参与计数的 i16 值个数（re+im 各计一次）。
    pub samples: usize,
    pub bin_width: f64,
    pub min: i32,
    pub max: i32,
    /// 每个 bin 的计数，长度 = (max-min+1)/bin_width = 16384。
    pub counts: Vec<u64>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct CaptureResult {
    /// 设备已使能（dst_mac != 0）且本次尝试接收的 port
    pub enabled_ports: Vec<usize>,
    /// 设备未使能（dst_mac 全 0），设备不会发送该路数据，已跳过
    pub skipped_ports: Vec<usize>,
    /// 实际收到至少一帧并成功出谱的 port
    pub spectra: Vec<PortSpectrum>,
    /// 有数据的通道的原始 i16 值直方图（仅包含实际收到数据的通道）
    pub histograms: Vec<PortHistogram>,
}

#[derive(Clone, Debug)]
pub struct CaptureHandle {
    pub abort: Arc<AtomicBool>,
    pub phase: Arc<Mutex<String>>,
    pub progress: Arc<Mutex<Vec<PortProgress>>>,
    pub done: Arc<AtomicBool>,
    pub result: Arc<Mutex<Option<Result<CaptureResult, String>>>>,
}

impl CaptureHandle {
    pub fn new(frames_per_port: usize) -> Self {
        CaptureHandle {
            abort: Arc::new(AtomicBool::new(false)),
            phase: Arc::new(Mutex::new("queued".to_string())),
            progress: Arc::new(Mutex::new(
                (0..8)
                    .map(|port| PortProgress {
                        port,
                        received: 0,
                        total: frames_per_port,
                    })
                    .collect(),
            )),
            done: Arc::new(AtomicBool::new(false)),
            result: Arc::new(Mutex::new(None)),
        }
    }

    pub fn set_phase(&self, s: &str) {
        *self.phase.lock().unwrap() = s.to_string();
    }
}

/// 同步采集所有 port：先拉起抓取线程并武装，等全部就绪后发 `StreamStart`，随后放行所有线程
/// 同拍接收；收满 `frames_per_port` 帧后 `StreamStop`，再对每个 port 做原始满带宽 FFT。
/// 全程支持超时与 `handle.abort` 外部取消。
pub fn run_capture(
    ip: Ipv4Addr,
    spec: &CaptureSpec,
    handle: &CaptureHandle,
    ctrl_lock: Arc<std::sync::Mutex<()>>,
    lo_mhz: f64,
) {
    let res = capture_inner(ip, spec, handle, ctrl_lock, lo_mhz);
    *handle.result.lock().unwrap() = Some(res);
    handle.done.store(true, Ordering::SeqCst);
}

fn capture_inner(
    ip: Ipv4Addr,
    spec: &CaptureSpec,
    handle: &CaptureHandle,
    ctrl_lock: Arc<std::sync::Mutex<()>>,
    lo_mhz: f64,
) -> Result<CaptureResult, String> {
    handle.set_phase("discover");
    let info = {
        let _g = ctrl_lock.lock().unwrap_or_else(|e| e.into_inner());
        get_device_info(ip)
    }
    .ok_or_else(|| "no device info for this ip (未发现设备)".to_string())?;

    let ctrl_remote = match info.ctrl_addr {
        std::net::SocketAddr::V4(a) => a,
        _ => return Err("control address is not IPv4".into()),
    };
    let ctrl = SdrCtrl {
        remote_ctrl_addr: ctrl_remote,
        local_ctrl_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, spec.local_ctrl_port),
    };

    handle.set_phase("query-rate");
    let smp_rate_mhz = {
        let _g = ctrl_lock.lock().unwrap_or_else(|e| e.into_inner());
        ctrl.query()
            .normal_reply
            .iter()
            .find_map(|(a, m)| parse_status(a, m).map(|s| s.health.smp_rate as f64))
            .unwrap_or(0.0)
    };
    // 带宽必须来自 Query；拿不到采样率就报错，强制设备已发现/在线。
    if smp_rate_mhz <= 0.0 {
        return Err("设备未回复 Query，无法确定采样率/带宽；请先发现并选择设备（或确认设备在线）。".into());
    }

    // 探测 Xgbe 配置：dst_mac 全 0 ⇒ 设备不发送该路 ⇒ 跳过；只对使能 port 绑定并等待。
    let cfg = {
        let _g = ctrl_lock.lock().unwrap_or_else(|e| e.into_inner());
        xgbe_cfg(info.ctrl_addr, spec.local_ctrl_port, spec.timeout_ms)
    }?;
    let mut ports: Vec<(usize, SocketAddrV4)> = Vec::new();
    let mut enabled_ports = Vec::new();
    let mut skipped_ports = Vec::new();
    for i in 0..8 {
        let mac0 = cfg.get(i).map(|c| c.dst_mac == [0u8; 6]).unwrap_or(true);
        let pa = info.payload_addr.get(i).and_then(|o| o.clone());
        if mac0 {
            skipped_ports.push(i);
            continue;
        }
        // 使能：优先用 host 已匹配的 payload 地址；否则退回 cfg 给出的 dst（主机可能仍有该接口）。
        let addr = match pa {
            Some(a) => a,
            None => match cfg.get(i) {
                Some(c) => SocketAddrV4::new(Ipv4Addr::from(c.dst_ip), c.dst_port),
                None => {
                    skipped_ports.push(i);
                    continue;
                }
            },
        };
        enabled_ports.push(i);
        ports.push((i, addr));
    }
    if ports.is_empty() {
        return Err(
            "所有 Xgbe port 的 dst_mac 均为 0，设备未使能任何数据通路（或未完成初始化）".into(),
        );
    }
    // 被跳过的 port 在进度里标记为 0/0，前端显示为 skipped
    {
        let mut prog = handle.progress.lock().unwrap();
        for &p in &skipped_ports {
            if let Some(e) = prog.iter_mut().find(|e| e.port == p) {
                e.total = 0;
            }
        }
    }

    let n = ports.len();
    handle.set_phase(&format!("arm ({n} ports)"));
    let abort = handle.abort.clone();
    let shared_progress = handle.progress.clone();
    let armed = Arc::new(AtomicUsize::new(0));
    let sample_slots: Vec<Arc<Mutex<Vec<Complex<i16>>>>> = (0..8)
        .map(|_| Arc::new(Mutex::new(Vec::<Complex<i16>>::new())))
        .collect();

    let (go_txs, go_rxs): (Vec<_>, Vec<_>) =
        (0..n).map(|_| std::sync::mpsc::channel::<()>()).unzip();
    let mut go_rx_iter = go_rxs.into_iter();

    let mut handles = Vec::with_capacity(n);
    for (_idx, (port_id, addr)) in ports.into_iter().enumerate() {
        let socket = UdpSocket::bind(addr).map_err(|e| format!("bind {addr}: {e}"))?;
        let _ = set_recv_buffer_size(&socket, RX_SOCKET_BUFFER);
        let _ = socket.set_read_timeout(Some(Duration::from_millis(RECV_POLL_MS)));

        let go_rx = go_rx_iter.next().expect("missing go receiver");
        let abort = abort.clone();
        let progress = shared_progress.clone();
        let slot = sample_slots[port_id].clone();
        let armed = armed.clone();
        let total = spec.frames_per_port;
        let timeout_ms = spec.timeout_ms;
        let port = port_id;

        handles.push(thread::spawn(move || {
            // 已绑好 socket，进入“武装就绪”状态
            armed.fetch_add(1, Ordering::SeqCst);
            // 等待协调者发送 StreamStart 后的 "go"，确保同拍开收
            if go_rx.recv().is_err() {
                updated_progress(&progress, port, 0);
                return;
            }
            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
            let mut local: Vec<Complex<i16>> = Vec::with_capacity(total * PATCH_LEN);
            let mut buf: Payload<Complex<i16>> = Payload::default();
            let mut received = 0usize;

            while !abort.load(Ordering::Relaxed)
                && received < total
                && Instant::now() < deadline
            {
                let buf_u8 = as_mut_u8_slice(&mut buf as &mut Payload<Complex<i16>>);
                match socket.recv_from(buf_u8) {
                    Ok((sz, _)) => {
                        if sz != std::mem::size_of::<Payload<Complex<i16>>>() {
                            continue;
                        }
                        local.extend_from_slice(&buf.data);
                        received += 1;
                        if received % 25 == 0 {
                            updated_progress(&progress, port, received);
                        }
                    }
                    Err(_) => {
                        // 读超时：循环继续，交给 abort / deadline 判断
                        updated_progress(&progress, port, received);
                    }
                }
            }
            *slot.lock().unwrap() = local;
            updated_progress(&progress, port, received);
        }));
    }

    // 等待全部武装
    let arm_deadline = Instant::now() + Duration::from_secs(ARM_TIMEOUT_SECS);
    let mut all_armed = false;
    while Instant::now() < arm_deadline {
        if handle.abort.load(Ordering::SeqCst) {
            abort.store(true, Ordering::SeqCst);
            join_all(handles);
            return Err("已取消".into());
        }
        if armed.load(Ordering::SeqCst) >= n {
            all_armed = true;
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    if !all_armed {
        abort.store(true, Ordering::SeqCst);
        join_all(handles);
        return Err("部分抓取端口未能就绪".into());
    }

    // 先 StreamStart
    handle.set_phase("stream-start");
    {
        let _g = ctrl_lock.lock().unwrap_or_else(|e| e.into_inner());
        ctrl.stream_start();
    }

    // 放行全部线程
    handle.set_phase("capture");
    for tx in go_txs {
        let _ = tx.send(());
    }

    join_all(handles);
    {
        let _g = ctrl_lock.lock().unwrap_or_else(|e| e.into_inner());
        let _ = ctrl.stream_stop();
    }

    if handle.abort.load(Ordering::SeqCst) {
        return Err("已取消".into());
    }

    handle.set_phase("analyze");
    let mut spectra = Vec::with_capacity(8);
    let mut histograms = Vec::with_capacity(8);
    for port_id in 0..8 {
        let samples = sample_slots[port_id].lock().unwrap();
        if samples.is_empty() {
            continue;
        }
        // 同一批原始数据，同时算谱分析与原始值直方图
        spectra.push(spectrum_of(port_id, &samples, spec, smp_rate_mhz, lo_mhz));
        histograms.push(histogram_of(port_id, &samples));
    }
    if spectra.is_empty() {
        return Err(format!(
            "未收到任何数据帧（已使能端口 {:?}，可能设备未起流或配置异常）",
            enabled_ports
        ));
    }
    handle.set_phase("done");
    Ok(CaptureResult {
        enabled_ports,
        skipped_ports,
        spectra,
        histograms,
    })
}

fn updated_progress(progress: &Arc<Mutex<Vec<PortProgress>>>, port_id: usize, received: usize) {
    if let Ok(mut p) = progress.lock() {
        if let Some(e) = p.iter_mut().find(|e| e.port == port_id) {
            e.received = received;
        }
    }
}

fn join_all(handles: Vec<thread::JoinHandle<()>>) {
    for h in handles {
        let _ = h.join();
    }
}

fn next_pow2(n: usize) -> usize {
    let mut p = 1usize;
    while p < n {
        p <<= 1;
    }
    p
}

fn make_window(window: Window, n: usize) -> Vec<f32> {
    match window {
        Window::Hann => (0..n)
            .map(|i| 0.5 - 0.5 * (2.0 * std::f64::consts::PI * i as f64 / (n as f64 - 1.0)).cos())
            .map(|x| x as f32)
            .collect(),
        Window::Rect => vec![1.0f32; n],
    }
}

fn spectrum_of(
    port_id: usize,
    samples: &[Complex<i16>],
    spec: &CaptureSpec,
    smp_rate_mhz: f64,
    lo_mhz: f64,
) -> PortSpectrum {
    let fft_size = next_pow2(spec.fft_size).min(1 << 20);
    let n = samples.len();
    let blocks = n / fft_size;
    let window = make_window(spec.window, fft_size);
    let win_pow: f32 = window.iter().map(|w| w * w).sum();
    let win_norm = if win_pow > 0.0 { 1.0 / win_pow.sqrt() } else { 1.0 };

    let mut planner = FftPlanner::new();
    let fft = planner.plan_fft_forward(fft_size);
    // 全带宽功率谱（复数：所有 fft_size 个 bin 均有效，涵盖 -fs/2..+fs/2）
    let mut acc = vec![0.0f64; fft_size];
    let mut buf = vec![Complex::new(0.0f32, 0.0f32); fft_size];

    let mut used_blocks = 0usize;
    for b in 0..blocks.max(1) {
        let start = b * fft_size;
        if start + fft_size > n {
            break;
        }
        for i in 0..fft_size {
            let w = window[i] * win_norm;
            buf[i] = Complex::new(
                samples[start + i].re as f32 * w,
                samples[start + i].im as f32 * w,
            );
        }
        fft.process(&mut buf);
        for k in 0..fft_size {
            acc[k] += buf[k].norm_sqr() as f64;
        }
        used_blocks += 1;
    }
    let used_blocks = used_blocks.max(1);

    // 居中排列（fftshift）：bins[0] = -fs/2，bins[N/2] = DC，bins[N-1] = +fs/2 - Δ
    let full: Vec<f32> = acc
        .iter()
        .map(|&p| {
            let p = p / used_blocks as f64;
            let mag = p.sqrt();
            20.0 * (mag.max(1e-12)).log10() as f32
        })
        .collect();
    let half = fft_size / 2;
    let mut bins = vec![0.0f32; fft_size];
    for k in 0..fft_size {
        bins[k] = full[(k + half) % fft_size];
    }

    let bin_width_hz = if smp_rate_mhz > 0.0 {
        smp_rate_mhz * 1e6 / fft_size as f64
    } else {
        0.0
    };
    let duration_ms = if smp_rate_mhz > 0.0 {
        (n as f64 / PATCH_LEN as f64) * (PATCH_LEN as f64) / (smp_rate_mhz * 1e6) * 1e3
    } else {
        0.0
    };

    PortSpectrum {
        port: port_id,
        samples_used: used_blocks * fft_size,
        blocks: used_blocks,
        bins,
        bin_width_hz,
        smp_rate_mhz,
        lo_mhz,
        duration_ms,
    }
}

/// 原始 i16 值直方图：横轴固定 -32768..+32767，bin 宽 4（共 16384 个 bin）。
/// 每个复采样取 I、Q 两个 i16 值各计一次。
fn histogram_of(port_id: usize, samples: &[Complex<i16>]) -> PortHistogram {
    const MIN_VAL: i64 = -32768;
    const MAX_VAL: i64 = 32767;
    const BIN_WIDTH: i64 = 4;
    const NBINS: usize = ((MAX_VAL - MIN_VAL + 1) / BIN_WIDTH) as usize; // 16384

    let mut counts = vec![0u64; NBINS];
    for s in samples {
        for v in [s.re as i64, s.im as i64] {
            let idx = ((v - MIN_VAL) / BIN_WIDTH).clamp(0, (NBINS - 1) as i64) as usize;
            counts[idx] += 1;
        }
    }
    PortHistogram {
        port: port_id,
        samples: samples.len() * 2,
        bin_width: BIN_WIDTH as f64,
        min: MIN_VAL as i32,
        max: MAX_VAL as i32,
        counts,
    }
}
