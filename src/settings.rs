use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// 每端口 XGbe 配置（MAC/IP 以字符串保存，便于前端直接回填）。
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct XgbePortSetting {
    pub port: u32,
    pub dst_mac: String,
    pub src_mac: String,
    pub dst_ip: String,
    pub src_ip: String,
    pub dst_port: u16,
    pub src_port: u16,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DsaSetting {
    pub port: u32,
    pub dsa_value: f32,
}

fn default_eq() -> [i8; 4] {
    [5; 4]
}
fn default_adapt() -> [u8; 4] {
    [1; 4]
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct QsfpSetting {
    pub cdr_ctrl: u32,
    #[serde(default = "default_eq")]
    pub eq_ctrl: [i8; 4],
    #[serde(default = "default_adapt")]
    pub adapt_eq: [u8; 4],
}

/// 设备状态（持久化到后端文件）：XGbe 每端口 / DSA 每端口 / QSFP。
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DeviceSettings {
    #[serde(default)]
    pub xgbe: Vec<XgbePortSetting>,
    #[serde(default)]
    pub dsa: Vec<DsaSetting>,
    #[serde(default)]
    pub qsfp: QsfpSetting,
}

impl DeviceSettings {
    pub fn path() -> PathBuf {
        let mut p = crate::config::Config::path();
        p.set_file_name("settings.yaml");
        p
    }

    /// 加载设备状态；文件不存在或解析失败则用默认。
    pub fn load() -> Self {
        let path = Self::path();
        match std::fs::read_to_string(&path) {
            Ok(s) => serde_yaml::from_str(&s).unwrap_or_default(),
            Err(_) => Self::default(),
        }
    }

    pub fn save(&self) {
        let path = Self::path();
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        match serde_yaml::to_string(self) {
            Ok(s) => {
                let _ = std::fs::write(&path, s);
            }
            Err(e) => eprintln!("failed to serialize settings: {e}"),
        }
    }
}
