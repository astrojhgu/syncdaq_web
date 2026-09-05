use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub const DEFAULT_LOCAL_CTRL_PORT: u16 = 3001;
pub const DEFAULT_TIMEOUT_MS: u64 = 5000; // Sync/MixerSet 等命令实际需 ~3s，故取 5s 兜底
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 2000;
pub const DEFAULT_CAPTURE_FRAMES_PER_PORT: usize = 1000;
pub const DEFAULT_FFT_SIZE: usize = 8192;
pub const DEFAULT_CAPTURE_TIMEOUT_MS: u64 = 5000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureDefaults {
    #[serde(default = "default_capture_frames")]
    pub frames_per_port: usize,
    #[serde(default = "default_fft_size")]
    pub fft_size: usize,
    #[serde(default = "default_window")]
    pub window: String,
    #[serde(default = "default_capture_timeout_ms")]
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_local_ctrl_port")]
    pub local_ctrl_port: u16,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// 时钟源：'gps'（板上 GPS 模块 10M+PPS，clk_src=1/pps_src=0）或
    /// 'ext_clk'（外接同轴 10M+PPS，clk_src=2/pps_src=1）。默认 gps。
    #[serde(default = "default_clock_source")]
    pub clock_source: String,
    /// 本振（LO）频率（MHz），用于频谱 x 轴偏移与初始化本振：RF = lo_mhz + 基带频率。
    #[serde(default = "default_lo_mhz")]
    pub lo_mhz: f64,
    /// 最近选中的设备控制地址，形如 "10.100.11.20:3000"。
    #[serde(default)]
    pub selected_device: Option<String>,
    #[serde(default)]
    pub capture: CaptureDefaults,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            local_ctrl_port: DEFAULT_LOCAL_CTRL_PORT,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            clock_source: default_clock_source(),
            lo_mhz: default_lo_mhz(),
            selected_device: None,
            capture: CaptureDefaults::default(),
        }
    }
}

impl Default for CaptureDefaults {
    fn default() -> Self {
        Self {
            frames_per_port: DEFAULT_CAPTURE_FRAMES_PER_PORT,
            fft_size: DEFAULT_FFT_SIZE,
            window: default_window(),
            timeout_ms: DEFAULT_CAPTURE_TIMEOUT_MS,
        }
    }
}

fn default_local_ctrl_port() -> u16 {
    DEFAULT_LOCAL_CTRL_PORT
}
fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}
fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}
fn default_clock_source() -> String {
    "gps".to_string()
}
fn default_lo_mhz() -> f64 {
    360.0
}
fn default_capture_frames() -> usize {
    DEFAULT_CAPTURE_FRAMES_PER_PORT
}
fn default_fft_size() -> usize {
    DEFAULT_FFT_SIZE
}
fn default_capture_timeout_ms() -> u64 {
    DEFAULT_CAPTURE_TIMEOUT_MS
}
fn default_window() -> String {
    "hann".to_string()
}

impl Config {
    /// 配置路径：`$XDG_CONFIG_HOME/syncdaq_web/config.yaml` 或 `~/.config/syncdaq_web/config.yaml`。
    pub fn path() -> PathBuf {
        if let Ok(x) = std::env::var("XDG_CONFIG_HOME") {
            let p = PathBuf::from(x).join("syncdaq_web").join("config.yaml");
            if !p.as_os_str().is_empty() {
                return p;
            }
        }
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home)
                .join(".config")
                .join("syncdaq_web")
                .join("config.yaml");
        }
        PathBuf::from("syncdaq_web/config.yaml")
    }

    pub fn load() -> Self {
        let path = Self::path();
        match std::fs::read_to_string(&path) {
            Ok(s) => serde_yaml::from_str(&s).unwrap_or_else(|e| {
                eprintln!("config parse error ({e}); using defaults");
                Config::default()
            }),
            Err(_) => {
                let cfg = Config::default();
                cfg.save();
                cfg
            }
        }
    }

    pub fn save(&self) {
        let path = Self::path();
        if let Some(dir) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(dir) {
                eprintln!("failed to create config dir {dir:?}: {e}");
                return;
            }
        }
        match serde_yaml::to_string(self) {
            Ok(s) => {
                if let Err(e) = std::fs::write(&path, s) {
                    eprintln!("failed to write config {path:?}: {e}");
                }
            }
            Err(e) => eprintln!("failed to serialize config: {e}"),
        }
    }
}
