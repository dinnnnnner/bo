use eframe::egui;
use serialport::{SerialPort, SerialPortType};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::{self, Read, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, Sender},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const BAUD_RATE: u32 = 115_200;
const SCAN_INTERVAL: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_LOG_LINES: usize = 300;
const FRAME_LEN: usize = 14;
const FRAME_PREFIX: [u8; 2] = [0xAA, 0x55];
const STATUS_FRAME_TYPE: u8 = 0x81;
const SELF_TEST_RESULT_FRAME_TYPE: u8 = 0x83;
const FRAME_TAIL: [u8; 2] = [0x55, 0xAA];
const THRESHOLD_FRAME_HEAD: [u8; 3] = [0xAA, 0x55, 0x01];
const SELF_TEST_START_FRAME: &[u8] = &[0xAA, 0x55, 0x02, 0x55, 0xAA];
const SELF_TEST_STOP_FRAME: &[u8] = &[0xAA, 0x55, 0x03, 0x55, 0xAA];
const STANDBY_START_FRAME: &[u8] = &[0xAA, 0x55, 0x04, 0x55, 0xAA];
const STANDBY_STOP_FRAME: &[u8] = &[0xAA, 0x55, 0x05, 0x55, 0xAA];
const SENSOR_NAMES: [&str; 3] = ["S1", "S2", "S3"];
const AXIS_NAMES: [&str; 3] = ["X", "Y", "Z"];
const THRESHOLD_LABELS: [&str; 6] = [
    "X上限",
    "X下限",
    "Y上限",
    "Y下限",
    "Z上限",
    "Z下限",
];

#[derive(Debug)]
enum PortCommand {
    Write(Vec<u8>),
    Shutdown,
}

#[derive(Debug)]
enum PortEvent {
    Connected(String),
    Data { port: String, data: Vec<u8> },
    Disconnected { port: String, reason: String },
}

#[derive(Clone)]
struct SerialManager {
    ports: Arc<Mutex<HashMap<String, Sender<PortCommand>>>>,
}

impl SerialManager {
    fn new() -> Self {
        Self {
            ports: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn start(&self, event_tx: Sender<PortEvent>) {
        let ports = Arc::clone(&self.ports);

        thread::spawn(move || {
            loop {
                match serialport::available_ports() {
                    Ok(available_ports) => {
                        let current: HashSet<String> = available_ports
                            .iter()
                            .map(|p| p.port_name.clone())
                            .collect();

                        remove_missing_ports(&ports, &current, &event_tx);

                        for info in available_ports {
                            let mut managed_ports = ports.lock().expect("ports lock poisoned");
                            if managed_ports.contains_key(&info.port_name) {
                                continue;
                            }

                            let port_name = info.port_name.clone();
                            let (command_tx, command_rx) = mpsc::channel();
                            managed_ports.insert(port_name.clone(), command_tx);
                            drop(managed_ports);

                            print_port_info(&info);
                            spawn_port_worker(
                                port_name,
                                command_rx,
                                event_tx.clone(),
                                Arc::clone(&ports),
                            );
                        }
                    }
                    Err(err) => eprintln!("scan serial ports failed: {err}"),
                }

                thread::sleep(SCAN_INTERVAL);
            }
        });
    }

    fn send_to(&self, port_name: &str, data: impl Into<Vec<u8>>) -> io::Result<()> {
        let command_tx = {
            let ports = self.ports.lock().expect("ports lock poisoned");
            ports.get(port_name).cloned()
        };

        match command_tx {
            Some(tx) => tx
                .send(PortCommand::Write(data.into()))
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "serial worker stopped")),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("serial port not managed: {port_name}"),
            )),
        }
    }

    fn send_all(&self, data: impl Into<Vec<u8>>) {
        let data = data.into();
        let ports = self.ports.lock().expect("ports lock poisoned");

        for (port_name, command_tx) in ports.iter() {
            if command_tx.send(PortCommand::Write(data.clone())).is_err() {
                eprintln!("send to {port_name} failed: serial worker stopped");
            }
        }
    }

    fn list_ports(&self) -> Vec<String> {
        let ports = self.ports.lock().expect("ports lock poisoned");
        let mut names = ports.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }
}

fn remove_missing_ports(
    ports: &Arc<Mutex<HashMap<String, Sender<PortCommand>>>>,
    current: &HashSet<String>,
    event_tx: &Sender<PortEvent>,
) {
    let mut managed_ports = ports.lock().expect("ports lock poisoned");
    let missing: Vec<String> = managed_ports
        .keys()
        .filter(|port_name| !current.contains(*port_name))
        .cloned()
        .collect();

    for port_name in missing {
        if let Some(command_tx) = managed_ports.remove(&port_name) {
            let _ = command_tx.send(PortCommand::Shutdown);
            let _ = event_tx.send(PortEvent::Disconnected {
                port: port_name,
                reason: "device removed".to_string(),
            });
        }
    }
}

fn spawn_port_worker(
    port_name: String,
    command_rx: Receiver<PortCommand>,
    event_tx: Sender<PortEvent>,
    managed_ports: Arc<Mutex<HashMap<String, Sender<PortCommand>>>>,
) {
    thread::spawn(move || {
        let mut port = match open_port(&port_name) {
            Ok(port) => {
                let _ = event_tx.send(PortEvent::Connected(port_name.clone()));
                port
            }
            Err(err) => {
                let _ = event_tx.send(PortEvent::Disconnected {
                    port: port_name.clone(),
                    reason: format!("open failed: {err}"),
                });
                cleanup_port(&managed_ports, &port_name);
                return;
            }
        };

        let mut read_buf = [0_u8; 1024];

        loop {
            while let Ok(command) = command_rx.try_recv() {
                match command {
                    PortCommand::Write(data) => {
                        if let Err(err) = port.write_all(&data) {
                            let _ = event_tx.send(PortEvent::Disconnected {
                                port: port_name.clone(),
                                reason: format!("write failed: {err}"),
                            });
                            cleanup_port(&managed_ports, &port_name);
                            return;
                        }
                        let _ = port.flush();
                    }
                    PortCommand::Shutdown => return,
                }
            }

            match port.read(&mut read_buf) {
                Ok(n) if n > 0 => {
                    let _ = event_tx.send(PortEvent::Data {
                        port: port_name.clone(),
                        data: read_buf[..n].to_vec(),
                    });
                }
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                Err(err) => {
                    let _ = event_tx.send(PortEvent::Disconnected {
                        port: port_name.clone(),
                        reason: format!("read failed: {err}"),
                    });
                    cleanup_port(&managed_ports, &port_name);
                    return;
                }
            }
        }
    });
}

fn cleanup_port(ports: &Arc<Mutex<HashMap<String, Sender<PortCommand>>>>, port_name: &str) {
    let mut managed_ports = ports.lock().expect("ports lock poisoned");
    managed_ports.remove(port_name);
}

fn open_port(port_name: &str) -> serialport::Result<Box<dyn SerialPort>> {
    serialport::new(port_name, BAUD_RATE)
        .timeout(READ_TIMEOUT)
        .open()
}

fn print_port_info(info: &serialport::SerialPortInfo) {
    match &info.port_type {
        SerialPortType::UsbPort(usb) => {
            println!(
                "found {} USB vid={:04x} pid={:04x}",
                info.port_name, usb.vid, usb.pid
            );
        }
        SerialPortType::BluetoothPort => println!("found {} Bluetooth", info.port_name),
        SerialPortType::PciPort => println!("found {} PCI", info.port_name),
        SerialPortType::Unknown => println!("found {}", info.port_name),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AlarmState {
    Normal,
    Alarm,
    Invalid(u8),
}

impl AlarmState {
    fn from_byte(value: u8) -> Self {
        match value {
            0x00 => Self::Normal,
            0x01 => Self::Alarm,
            other => Self::Invalid(other),
        }
    }

    fn label(self) -> String {
        match self {
            Self::Normal => "正常".to_string(),
            Self::Alarm => "报警".to_string(),
            Self::Invalid(value) => format!("无效 0x{value:02X}"),
        }
    }

    fn color(self) -> egui::Color32 {
        match self {
            Self::Normal => egui::Color32::from_rgb(38, 166, 91),
            Self::Alarm => egui::Color32::from_rgb(220, 53, 69),
            Self::Invalid(_) => egui::Color32::from_rgb(245, 158, 11),
        }
    }
}

impl Default for AlarmState {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Clone, Copy)]
enum SelfTestAction {
    Start,
    Stop,
}

impl SelfTestAction {
    fn label(self) -> &'static str {
        match self {
            Self::Start => "开始",
            Self::Stop => "停止",
        }
    }
}

#[derive(Clone, Copy)]
enum StandbyAction {
    Start,
    Stop,
}

impl StandbyAction {
    fn label(self) -> &'static str {
        match self {
            Self::Start => "开始",
            Self::Stop => "停止",
        }
    }
}

#[derive(Clone, Debug)]
struct SensorFrame {
    port: String,
    received_at: Instant,
    count: u64,
    states: [[AlarmState; 3]; 3],
    raw_hex: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ParsedFrameKind {
    Status,
    SelfTestResult,
}

#[derive(Debug)]
struct ParsedFrame {
    kind: ParsedFrameKind,
    states: [[AlarmState; 3]; 3],
    raw: Vec<u8>,
}

#[derive(Default)]
struct FrameParser {
    buffer: Vec<u8>,
}

impl FrameParser {
    fn push(&mut self, data: &[u8]) -> Vec<ParsedFrame> {
        self.buffer.extend_from_slice(data);
        let mut frames = Vec::new();

        loop {
            let Some((pos, kind)) = find_frame_header(&self.buffer) else {
                if self.buffer.len() > FRAME_PREFIX.len() {
                    let keep = FRAME_PREFIX.len();
                    let drop_len = self.buffer.len() - keep;
                    self.buffer.drain(..drop_len);
                }
                break;
            };

            if pos > 0 {
                self.buffer.drain(..pos);
            }

            if self.buffer.len() < FRAME_LEN {
                break;
            }

            if self.buffer[FRAME_LEN - 2..FRAME_LEN] != FRAME_TAIL {
                self.buffer.drain(..1);
                continue;
            }

            let mut states = [[AlarmState::Normal; 3]; 3];
            let payload = &self.buffer[3..12];
            for sensor_idx in 0..3 {
                for axis_idx in 0..3 {
                    states[sensor_idx][axis_idx] =
                        AlarmState::from_byte(payload[sensor_idx * 3 + axis_idx]);
                }
            }

            frames.push(ParsedFrame {
                kind,
                states,
                raw: self.buffer[..FRAME_LEN].to_vec(),
            });
            self.buffer.drain(..FRAME_LEN);
        }

        frames
    }
}

fn find_frame_header(bytes: &[u8]) -> Option<(usize, ParsedFrameKind)> {
    bytes.windows(3).enumerate().find_map(|(idx, window)| {
        if window[0..2] != FRAME_PREFIX {
            return None;
        }

        match window[2] {
            STATUS_FRAME_TYPE => Some((idx, ParsedFrameKind::Status)),
            SELF_TEST_RESULT_FRAME_TYPE => Some((idx, ParsedFrameKind::SelfTestResult)),
            _ => None,
        }
    })
}

struct SerialPanelApp {
    manager: SerialManager,
    event_rx: Receiver<PortEvent>,
    parsers: HashMap<String, FrameParser>,
    connected_ports: HashSet<String>,
    selected_port: String,
    send_text: String,
    send_as_hex: bool,
    threshold_inputs: [String; 6],
    threshold_message: String,
    self_test_message: String,
    standby_message: String,
    latest_frames: HashMap<String, SensorFrame>,
    self_test_frames: HashMap<String, SensorFrame>,
    frame_counts: HashMap<String, u64>,
    self_test_counts: HashMap<String, u64>,
    frame_count: u64,
    standby_active: bool,
    invalid_send_message: String,
    log_save_message: String,
    logs: VecDeque<String>,
}

impl SerialPanelApp {
    fn new(manager: SerialManager, event_rx: Receiver<PortEvent>) -> Self {
        Self {
            manager,
            event_rx,
            parsers: HashMap::new(),
            connected_ports: HashSet::new(),
            selected_port: String::new(),
            send_text: "AA 55 81 00 00 00 00 00 00 00 00 00 55 AA".to_string(),
            send_as_hex: true,
            threshold_inputs: std::array::from_fn(|_| "0".to_string()),
            threshold_message: String::new(),
            self_test_message: String::new(),
            standby_message: String::new(),
            latest_frames: HashMap::new(),
            self_test_frames: HashMap::new(),
            frame_counts: HashMap::new(),
            self_test_counts: HashMap::new(),
            frame_count: 0,
            standby_active: false,
            invalid_send_message: String::new(),
            log_save_message: String::new(),
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
        }
    }

    fn push_log(&mut self, line: impl Into<String>) {
        if self.logs.len() >= MAX_LOG_LINES {
            let _ = self.logs.pop_front();
        }
        self.logs.push_back(line.into());
    }

    fn drain_serial_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                PortEvent::Connected(port) => {
                    self.connected_ports.insert(port.clone());
                    if self.selected_port.is_empty() {
                        self.selected_port = port.clone();
                    }
                    self.push_log(format!("{port} 已连接"));
                }
                PortEvent::Data { port, data } => {
                    if self.standby_active {
                        continue;
                    }

                    let parser = self.parsers.entry(port.clone()).or_default();
                    for parsed in parser.push(&data) {
                        match parsed.kind {
                            ParsedFrameKind::Status => {
                                self.frame_count = self.frame_count.saturating_add(1);
                                let port_count = self.frame_counts.entry(port.clone()).or_insert(0);
                                *port_count = port_count.saturating_add(1);
                                let frame = SensorFrame {
                                    port: port.clone(),
                                    received_at: Instant::now(),
                                    count: *port_count,
                                    states: parsed.states,
                                    raw_hex: hex_string(&parsed.raw),
                                };
                                if let Some(summary) = alarm_summary(&parsed.states) {
                                    self.push_log(format!(
                                        "{} [{}] 报警: {} | {}",
                                        now_string(),
                                        port,
                                        summary,
                                        frame.raw_hex
                                    ));
                                }
                                self.latest_frames.insert(port.clone(), frame);
                            }
                            ParsedFrameKind::SelfTestResult => {
                                let self_test_count =
                                    self.self_test_counts.entry(port.clone()).or_insert(0);
                                *self_test_count = self_test_count.saturating_add(1);
                                let frame = SensorFrame {
                                    port: port.clone(),
                                    received_at: Instant::now(),
                                    count: *self_test_count,
                                    states: parsed.states,
                                    raw_hex: hex_string(&parsed.raw),
                                };
                                self.self_test_message = format_self_test_result_line(&frame);
                                self.push_log(format!(
                                    "{} {} | {}",
                                    now_string(),
                                    self.self_test_message,
                                    frame.raw_hex
                                ));
                                self.self_test_frames.insert(port.clone(), frame);
                            }
                        }
                    }
                }
                PortEvent::Disconnected { port, reason } => {
                    self.connected_ports.remove(&port);
                    self.parsers.remove(&port);
                    self.latest_frames.remove(&port);
                    self.self_test_frames.remove(&port);
                    self.frame_counts.remove(&port);
                    self.self_test_counts.remove(&port);
                    if self.selected_port == port {
                        self.selected_port.clear();
                    }
                    self.push_log(format!("{port} 已断开: {reason}"));
                }
            }
        }

        let ports = self.manager.list_ports();
        if self.selected_port.is_empty() {
            if let Some(port) = ports.first() {
                self.selected_port = port.clone();
            }
        }
    }

    fn send_current(&mut self, all_ports: bool) {
        self.invalid_send_message.clear();
        let data = if self.send_as_hex {
            match parse_hex_bytes(&self.send_text) {
                Ok(data) => data,
                Err(err) => {
                    self.invalid_send_message = err;
                    return;
                }
            }
        } else {
            self.send_text.as_bytes().to_vec()
        };

        if all_ports {
            self.manager.send_all(data);
            self.push_log("已发送到全部串口");
            return;
        }

        let port = self.selected_port.trim().to_string();
        if port.is_empty() {
            self.invalid_send_message = "请先选择串口".to_string();
            return;
        }

        match self.manager.send_to(&port, data) {
            Ok(()) => self.push_log(format!("已发送到 {port}")),
            Err(err) => self.invalid_send_message = format!("发送失败: {err}"),
        }
    }

    fn send_thresholds_to_all(&mut self) {
        self.threshold_message.clear();

        let mut values = [0_i16; 6];
        for (idx, input) in self.threshold_inputs.iter().enumerate() {
            match input.trim().parse::<i16>() {
                Ok(value) => values[idx] = value,
                Err(_) => {
                    self.threshold_message =
                        format!("{} 必须是 int16，范围 -32768..32767", THRESHOLD_LABELS[idx]);
                    return;
                }
            }
        }

        let frame = build_threshold_frame(values);
        let hex = hex_string(&frame);
        self.manager.send_all(frame);
        self.threshold_message = format!("已发送阈值帧到全部串口: {hex}");
        self.push_log(self.threshold_message.clone());
    }

    fn send_self_test_frame(&mut self, action: SelfTestAction) {
        let frame = match action {
            SelfTestAction::Start => SELF_TEST_START_FRAME,
            SelfTestAction::Stop => SELF_TEST_STOP_FRAME,
        };
        let hex = hex_string(frame);
        self.manager.send_all(frame.to_vec());
        self.self_test_message = format!("{}自检帧已发送到全部串口: {hex}", action.label());
        self.push_log(self.self_test_message.clone());
    }

    fn send_standby_frame(&mut self, action: StandbyAction) {
        let frame = match action {
            StandbyAction::Start => STANDBY_START_FRAME,
            StandbyAction::Stop => STANDBY_STOP_FRAME,
        };
        let hex = hex_string(frame);
        self.manager.send_all(frame.to_vec());
        self.standby_active = matches!(action, StandbyAction::Start);
        self.standby_message = format!("{}待机帧已发送到全部串口: {hex}", action.label());
        self.push_log(self.standby_message.clone());
    }

    #[allow(dead_code)]
    fn draw_sensor_grid(&self, ui: &mut egui::Ui) {
        let frames = self.sorted_frames();
        let Some(frame) = frames.first() else {
            ui.vertical_centered(|ui| {
                ui.add_space(80.0);
                ui.heading("等待串口数据");
                ui.label("期望帧: AA 55 81 s1x s1y s1z s2x s2y s2z s3x s3y s3z 55 AA");
            });
            return;
        };

        ui.horizontal(|ui| {
            ui.label(format!("来源: {}", frame.port));
            ui.separator();
            ui.label(format!("累计帧: {}", frame.count));
            ui.separator();
            ui.label(format!(
                "距上一帧: {} ms",
                frame.received_at.elapsed().as_millis()
            ));
        });

        ui.add_space(10.0);
        egui::Grid::new("sensor_grid")
            .num_columns(4)
            .spacing(egui::vec2(18.0, 10.0))
            .striped(true)
            .show(ui, |ui| {
                ui.strong("传感器");
                for axis in AXIS_NAMES {
                    ui.strong(axis);
                }
                ui.end_row();

                for (sensor_idx, sensor_name) in SENSOR_NAMES.iter().enumerate() {
                    ui.strong(*sensor_name);
                    for axis_idx in 0..3 {
                        let state = if self.standby_active {
                            AlarmState::Normal
                        } else {
                            frame.states[sensor_idx][axis_idx]
                        };
                        draw_state_badge(ui, state);
                    }
                    ui.end_row();
                }
            });

        ui.add_space(10.0);
        ui.label("最近原始数据");
        ui.monospace(&frame.raw_hex);
    }

    fn sorted_frames(&self) -> Vec<SensorFrame> {
        let mut frames = self.latest_frames.values().cloned().collect::<Vec<_>>();
        frames.sort_by(|a, b| a.port.cmp(&b.port));
        frames
    }

    fn sorted_self_test_frames(&self) -> Vec<SensorFrame> {
        let mut frames = self.self_test_frames.values().cloned().collect::<Vec<_>>();
        frames.sort_by(|a, b| a.port.cmp(&b.port));
        frames
    }

    fn draw_overview(&self, ui: &mut egui::Ui) {
        if self.latest_frames.is_empty() && self.self_test_frames.is_empty() {
            ui.vertical_centered(|ui| {
                ui.add_space(80.0);
                ui.heading("等待串口数据");
                ui.label("状态表只在各串口弹窗中显示");
            });
            return;
        }

        ui.heading("串口状态窗口");
        ui.label("每个串口的状态只显示在独立弹窗中。");
        ui.label(format!("总帧数: {}", self.frame_count));
        ui.add_space(8.0);
        for frame in self.sorted_frames() {
            ui.label(format!(
                "{}: {} 帧，距上一帧 {} ms",
                frame.port,
                frame.count,
                frame.received_at.elapsed().as_millis()
            ));
        }
    }

    fn draw_self_test_overview(&self, ui: &mut egui::Ui) {
        if self.self_test_frames.is_empty() {
            return;
        }

        ui.add_space(8.0);
        ui.separator();
        ui.heading("Self-test result");
        for frame in self.sorted_self_test_frames() {
            ui.label(format_self_test_result_line(&frame));
            ui.monospace(format!("Raw: {}", frame.raw_hex));
        }
    }

    fn save_logs_to_file(&mut self) {
        if self.logs.is_empty() {
            self.log_save_message = "没有可保存的日志".to_string();
            return;
        }

        let dir = Path::new("logs");
        if let Err(err) = fs::create_dir_all(dir) {
            self.log_save_message = format!("创建日志目录失败: {err}");
            return;
        }

        let timestamp = filename_time_string();
        let path = dir.join(format!("serial_log_{timestamp}.txt"));
        let content = self.logs.iter().cloned().collect::<Vec<_>>().join("\n");

        match fs::write(&path, content) {
            Ok(()) => {
                self.log_save_message = format!("日志已保存到 {}", path.display());
            }
            Err(err) => {
                self.log_save_message = format!("保存日志失败: {err}");
            }
        }
    }

    fn clear_logs(&mut self) {
        if self.logs.is_empty() {
            self.log_save_message = "当前日志已为空".to_string();
            return;
        }

        self.logs.clear();
        self.log_save_message = "当前日志已清除".to_string();
    }

    fn draw_logs(&mut self, ui: &mut egui::Ui) {
        ui.separator();
        ui.horizontal(|ui| {
            ui.heading("日志");
            if ui.button("保存日志").clicked() {
                self.save_logs_to_file();
            }
            if ui.button("清除日志").clicked() {
                self.clear_logs();
            }
        });
        if !self.log_save_message.is_empty() {
            ui.label(&self.log_save_message);
        }
        egui::ScrollArea::vertical()
            .stick_to_bottom(true)
            .show(ui, |ui| {
                for line in &self.logs {
                    ui.label(line);
                }
            });
    }

    fn draw_port_windows(&self, ctx: &egui::Context) {
        for (idx, frame) in self.sorted_frames().into_iter().enumerate() {
            let pos = egui::pos2(360.0 + idx as f32 * 36.0, 120.0 + idx as f32 * 36.0);
            egui::Window::new(format!("{} 状态", frame.port))
                .id(egui::Id::new(format!("port_status_window_{}", frame.port)))
                .default_pos(pos)
                .default_size(egui::vec2(380.0, 260.0))
                .resizable(false)
                .show(ctx, |ui| {
                    draw_sensor_grid(ui, &frame, self.standby_active);
                });
        }
    }
}

fn draw_sensor_grid(ui: &mut egui::Ui, frame: &SensorFrame, standby_active: bool) {
    ui.horizontal(|ui| {
        ui.label(format!("来源: {}", frame.port));
        ui.separator();
        ui.label(format!("累计帧: {}", frame.count));
        ui.separator();
        ui.label(format!(
            "距上一帧: {} ms",
            frame.received_at.elapsed().as_millis()
        ));
    });

    ui.add_space(10.0);
    egui::Grid::new(format!("sensor_grid_{}", frame.port))
        .num_columns(4)
        .spacing(egui::vec2(18.0, 10.0))
        .striped(true)
        .show(ui, |ui| {
            ui.strong("传感器");
            for axis in AXIS_NAMES {
                ui.strong(axis);
            }
            ui.end_row();

            for (sensor_idx, sensor_name) in SENSOR_NAMES.iter().enumerate() {
                ui.strong(*sensor_name);
                for axis_idx in 0..3 {
                    let state = if standby_active {
                        AlarmState::Normal
                    } else {
                        frame.states[sensor_idx][axis_idx]
                    };
                    draw_state_badge(ui, state);
                }
                ui.end_row();
            }
        });

    ui.add_space(10.0);
    ui.label("最近原始数据");
    ui.monospace(&frame.raw_hex);
}

impl eframe::App for SerialPanelApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_serial_events();

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.heading("串口报警面板");
            ui.horizontal_wrapped(|ui| {
                ui.label(format!("波特率: {BAUD_RATE}"));
                ui.separator();
                ui.label(format!("已管理串口: {}", self.manager.list_ports().len()));
                ui.separator();
                ui.label("解包: AA 55 81 + 9字节状态 + 55 AA");
            });
        });

        egui::SidePanel::left("controls")
            .resizable(false)
            .default_width(330.0)
            .show(ctx, |ui| {
                ui.heading("串口");
                let ports = self.manager.list_ports();
                egui::ComboBox::from_id_salt("serial_ports")
                    .selected_text(if self.selected_port.is_empty() {
                        "未选择".to_string()
                    } else {
                        self.selected_port.clone()
                    })
                    .show_ui(ui, |ui| {
                        for port in &ports {
                            ui.selectable_value(&mut self.selected_port, port.clone(), port);
                        }
                    });

                ui.add_space(12.0);
                ui.heading("发送");
                ui.horizontal(|ui| {
                    ui.radio_value(&mut self.send_as_hex, true, "HEX");
                    ui.radio_value(&mut self.send_as_hex, false, "文本");
                });
                ui.add(
                    egui::TextEdit::multiline(&mut self.send_text)
                        .desired_width(f32::INFINITY)
                        .desired_rows(4),
                );
                ui.horizontal(|ui| {
                    if ui.button("发送到选中串口").clicked() {
                        self.send_current(false);
                    }
                    if ui.button("发送到全部").clicked() {
                        self.send_current(true);
                    }
                });
                if !self.invalid_send_message.is_empty() {
                    ui.colored_label(
                        egui::Color32::from_rgb(220, 53, 69),
                        &self.invalid_send_message,
                    );
                }

                ui.separator();
                ui.heading("阈值设置");
                egui::Grid::new("threshold_grid")
                    .num_columns(2)
                    .spacing(egui::vec2(10.0, 8.0))
                    .show(ui, |ui| {
                        for (idx, label) in THRESHOLD_LABELS.iter().enumerate() {
                            ui.label(*label);
                            ui.add(
                                egui::TextEdit::singleline(&mut self.threshold_inputs[idx])
                                    .desired_width(96.0),
                            );
                            ui.end_row();
                        }
                    });
                if ui.button("设置阈值到全部串口").clicked() {
                    self.send_thresholds_to_all();
                }
                if !self.threshold_message.is_empty() {
                    ui.label(&self.threshold_message);
                }

                ui.separator();
                ui.heading("自检");
                ui.horizontal(|ui| {
                    if ui.button("开始自检").clicked() {
                        self.send_self_test_frame(SelfTestAction::Start);
                    }
                    if ui.button("停止自检").clicked() {
                        self.send_self_test_frame(SelfTestAction::Stop);
                    }
                });
                if !self.self_test_message.is_empty() {
                    ui.label(&self.self_test_message);
                }

                ui.separator();
                ui.heading("待机模式");
                ui.horizontal(|ui| {
                    if ui.button("开始待机").clicked() {
                        self.send_standby_frame(StandbyAction::Start);
                    }
                    if ui.button("停止待机").clicked() {
                        self.send_standby_frame(StandbyAction::Stop);
                    }
                });
                if !self.standby_message.is_empty() {
                    ui.label(&self.standby_message);
                }
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            self.draw_overview(ui);
            self.draw_self_test_overview(ui);
            self.draw_logs(ui);
        });
        self.draw_port_windows(ctx);

        ctx.request_repaint_after(Duration::from_millis(50));
    }
}

fn draw_state_badge(ui: &mut egui::Ui, state: AlarmState) {
    let text = state.label();
    let color = state.color();
    let button = egui::Button::new(text)
        .fill(color)
        .stroke(egui::Stroke::new(1.0, color))
        .min_size(egui::vec2(82.0, 32.0));
    ui.add_enabled(false, button);
}

fn alarm_summary(states: &[[AlarmState; 3]; 3]) -> Option<String> {
    let mut alarms = Vec::new();
    for (sensor_idx, sensor) in states.iter().enumerate() {
        for (axis_idx, state) in sensor.iter().enumerate() {
            if *state == AlarmState::Alarm {
                alarms.push(format!(
                    "{}{}",
                    SENSOR_NAMES[sensor_idx], AXIS_NAMES[axis_idx]
                ));
            }
        }
    }

    if alarms.is_empty() {
        None
    } else {
        Some(alarms.join(", "))
    }
}

fn format_self_test_result_line(frame: &SensorFrame) -> String {
    let mut items = Vec::new();
    for (sensor_idx, sensor) in frame.states.iter().enumerate() {
        for (axis_idx, state) in sensor.iter().enumerate() {
            if *state != AlarmState::Normal {
                items.push(format!(
                    "{}{}={}",
                    SENSOR_NAMES[sensor_idx],
                    AXIS_NAMES[axis_idx],
                    alarm_state_short_label(*state)
                ));
            }
        }
    }

    let result = if items.is_empty() {
        "OK".to_string()
    } else {
        items.join(", ")
    };

    format!(
        "{} self-test #{}: {} ({} ms ago)",
        frame.port,
        frame.count,
        result,
        frame.received_at.elapsed().as_millis()
    )
}

fn alarm_state_short_label(state: AlarmState) -> String {
    match state {
        AlarmState::Normal => "OK".to_string(),
        AlarmState::Alarm => "NG".to_string(),
        AlarmState::Invalid(value) => format!("Invalid 0x{value:02X}"),
    }
}

fn now_string() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format_local_time(duration.as_millis() as i64),
        Err(_) => "1970-01-01 00:00:00.000".to_string(),
    }
}

fn filename_time_string() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format_local_time_for_filename(duration.as_millis() as i64),
        Err(_) => "19700101_000000".to_string(),
    }
}

fn format_local_time(unix_ms: i64) -> String {
    let local_ms = unix_ms + 8 * 3600 * 1000;
    let total_seconds = local_ms.div_euclid(1000);
    let millis = local_ms.rem_euclid(1000);
    let days = total_seconds.div_euclid(86_400);
    let seconds_of_day = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;

    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{millis:03}")
}

fn format_local_time_for_filename(unix_ms: i64) -> String {
    let local_ms = unix_ms + 8 * 3600 * 1000;
    let total_seconds = local_ms.div_euclid(1000);
    let days = total_seconds.div_euclid(86_400);
    let seconds_of_day = total_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3600;
    let minute = (seconds_of_day % 3600) / 60;
    let second = seconds_of_day % 60;

    format!("{year:04}{month:02}{day:02}_{hour:02}{minute:02}{second:02}")
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

fn build_threshold_frame(values: [i16; 6]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(17);
    frame.extend_from_slice(&THRESHOLD_FRAME_HEAD);
    for value in values {
        frame.extend_from_slice(&value.to_le_bytes());
    }
    frame.extend_from_slice(&FRAME_TAIL);
    frame
}

fn parse_hex_bytes(text: &str) -> Result<Vec<u8>, String> {
    let compact = text
        .split(|ch: char| ch.is_whitespace() || ch == ',' || ch == ';')
        .filter(|part| !part.is_empty())
        .map(|part| part.trim_start_matches("0x").trim_start_matches("0X"))
        .collect::<Vec<_>>();

    if compact.is_empty() {
        return Err("HEX 内容不能为空".to_string());
    }

    let mut bytes = Vec::with_capacity(compact.len());
    for part in compact {
        if part.len() > 2 {
            return Err(format!("HEX 字节过长: {part}"));
        }
        let value = u8::from_str_radix(part, 16).map_err(|_| format!("无效 HEX 字节: {part}"))?;
        bytes.push(value);
    }
    Ok(bytes)
}

fn hex_string(data: &[u8]) -> String {
    data.iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn setup_chinese_fonts(ctx: &egui::Context) {
    let candidates = [
        "C:\\Windows\\Fonts\\msyh.ttc",
        "C:\\Windows\\Fonts\\msyhbd.ttc",
        "C:\\Windows\\Fonts\\simhei.ttf",
        "C:\\Windows\\Fonts\\simsun.ttc",
    ];

    for path in candidates {
        if !Path::new(path).exists() {
            continue;
        }
        let Ok(bytes) = fs::read(path) else {
            continue;
        };

        let mut fonts = egui::FontDefinitions::default();
        fonts.font_data.insert(
            "cn_font".to_owned(),
            egui::FontData::from_owned(bytes).into(),
        );
        fonts
            .families
            .entry(egui::FontFamily::Proportional)
            .or_default()
            .insert(0, "cn_font".to_owned());
        fonts
            .families
            .entry(egui::FontFamily::Monospace)
            .or_default()
            .push("cn_font".to_owned());
        ctx.set_fonts(fonts);
        break;
    }
}

fn main() -> eframe::Result<()> {
    let (event_tx, event_rx) = mpsc::channel();
    let manager = SerialManager::new();
    manager.start(event_tx);

    let options = eframe::NativeOptions::default();
    let app_creator = move |cc: &eframe::CreationContext<'_>| -> Result<
        Box<dyn eframe::App>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        setup_chinese_fonts(&cc.egui_ctx);
        Ok(Box::new(SerialPanelApp::new(manager.clone(), event_rx)))
    };

    eframe::run_native("serial_alarm_panel", options, Box::new(app_creator))
}
