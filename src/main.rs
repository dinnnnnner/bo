use eframe::egui;
use serialport::{SerialPort, SerialPortType};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
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
const AUTO_LOG_DIR: &str = "logs/auto";
const CURRENT_LOG_DIR: &str = "logs/current";
const MAX_TEMPLATE_ITEM_REPEAT_COUNT: usize = 10_000;
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
const THRESHOLD_LABELS: [&str; 6] = ["X上限", "X下限", "Y上限", "Y下限", "Z上限", "Z下限"];
const CUSTOM_PLAN_TEMPLATES_PATH: &str = "standby_plan_templates.txt";

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StandbyTimerStage {
    Standby,
    Normal,
}

impl StandbyTimerStage {
    fn label(self) -> &'static str {
        match self {
            Self::Normal => "正常模式",
            Self::Standby => "待机模式",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanFinishAction {
    Normal,
    Standby,
    KeepLast,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimerPageTab {
    Monitor,
    Editor,
}

impl TimerPageTab {
    fn label(self) -> &'static str {
        match self {
            Self::Monitor => "运行监控",
            Self::Editor => "计划编辑",
        }
    }
}

impl PlanFinishAction {
    fn label(self) -> &'static str {
        match self {
            Self::Normal => "正常模式",
            Self::Standby => "待机模式",
            Self::KeepLast => "保持最后状态",
        }
    }
}

#[derive(Clone, Debug)]
struct PlanTemplateItem {
    label: String,
    stage: StandbyTimerStage,
    duration: Duration,
}

impl PlanTemplateItem {
    fn new(label: impl Into<String>, stage: StandbyTimerStage, duration: Duration) -> Self {
        Self {
            label: label.into(),
            stage,
            duration,
        }
    }
}

#[derive(Clone, Debug)]
struct PlanTemplate {
    name: String,
    items: Vec<PlanTemplateItem>,
    finish_action: PlanFinishAction,
    is_custom: bool,
}

impl PlanTemplate {
    fn new(
        name: impl Into<String>,
        items: Vec<PlanTemplateItem>,
        finish_action: PlanFinishAction,
        is_custom: bool,
    ) -> Self {
        Self {
            name: name.into(),
            items,
            finish_action,
            is_custom,
        }
    }
}

#[derive(Clone, Debug)]
struct ScheduledModeEvent {
    label: String,
    stage: StandbyTimerStage,
    duration: Duration,
    started_at: Option<Instant>,
}

impl ScheduledModeEvent {
    fn from_template_item(item: &PlanTemplateItem) -> Self {
        Self {
            label: item.label.clone(),
            stage: item.stage,
            duration: item.duration,
            started_at: None,
        }
    }
}

#[derive(Clone, Debug)]
struct StandbyPlanRun {
    template_name: String,
    queue: VecDeque<ScheduledModeEvent>,
    current: Option<ScheduledModeEvent>,
    finish_action: PlanFinishAction,
    started_at: Instant,
    completed_events: u64,
}

impl StandbyPlanRun {
    fn from_template(template: &PlanTemplate) -> Self {
        let queue = template
            .items
            .iter()
            .map(ScheduledModeEvent::from_template_item)
            .collect();
        Self {
            template_name: template.name.clone(),
            queue,
            current: None,
            finish_action: template.finish_action,
            started_at: Instant::now(),
            completed_events: 0,
        }
    }

    fn pending_duration(&self) -> Duration {
        let current_remaining = self.current.as_ref().map_or(Duration::ZERO, |event| {
            duration_remaining(
                event.duration,
                event
                    .started_at
                    .map_or(Duration::ZERO, |started| started.elapsed()),
            )
        });
        self.queue
            .iter()
            .fold(current_remaining, |total, event| total + event.duration)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanResultStatus {
    Idle,
    Running,
    Completed,
    Interrupted,
}

impl PlanResultStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Idle => "未运行",
            Self::Running => "统计中",
            Self::Completed => "已完成",
            Self::Interrupted => "已中断",
        }
    }
}

#[derive(Clone, Debug)]
struct PlanResultStats {
    status: PlanResultStatus,
    template_name: String,
    started_at: Option<Instant>,
    elapsed: Duration,
    checked_frames: u64,
    alarm_frames: u64,
    alarm_points: u64,
    port_axis_alarm_counts: HashMap<String, [[u64; 3]; 3]>,
    last_alarm: String,
}

impl Default for PlanResultStats {
    fn default() -> Self {
        Self {
            status: PlanResultStatus::Idle,
            template_name: String::new(),
            started_at: None,
            elapsed: Duration::ZERO,
            checked_frames: 0,
            alarm_frames: 0,
            alarm_points: 0,
            port_axis_alarm_counts: HashMap::new(),
            last_alarm: String::new(),
        }
    }
}

impl PlanResultStats {
    fn start(&mut self, template_name: impl Into<String>) {
        *self = Self {
            status: PlanResultStatus::Running,
            template_name: template_name.into(),
            started_at: Some(Instant::now()),
            ..Self::default()
        };
    }

    fn finish(&mut self, status: PlanResultStatus) {
        if self.status == PlanResultStatus::Running {
            self.elapsed = self
                .started_at
                .map_or(Duration::ZERO, |started| started.elapsed());
        }
        self.status = status;
    }

    fn record_normal_frame(&mut self, port: &str, states: &[[AlarmState; 3]; 3]) {
        if self.status != PlanResultStatus::Running {
            return;
        }

        self.checked_frames = self.checked_frames.saturating_add(1);
        let alarm_points = count_alarm_points(states);
        if alarm_points == 0 {
            return;
        }

        self.alarm_frames = self.alarm_frames.saturating_add(1);
        self.alarm_points = self.alarm_points.saturating_add(alarm_points);
        let port_counts = self
            .port_axis_alarm_counts
            .entry(port.to_string())
            .or_default();
        for (sensor_idx, sensor) in states.iter().enumerate() {
            for (axis_idx, state) in sensor.iter().enumerate() {
                if *state == AlarmState::Alarm {
                    port_counts[sensor_idx][axis_idx] =
                        port_counts[sensor_idx][axis_idx].saturating_add(1);
                }
            }
        }
        if let Some(summary) = alarm_summary(states) {
            self.last_alarm = format!("{port}: {summary}");
        }
    }

    fn sorted_port_axis_counts(&self) -> Vec<(&str, usize, usize, u64)> {
        let mut counts = Vec::new();
        for (port, sensor_counts) in &self.port_axis_alarm_counts {
            for (sensor_idx, axis_counts) in sensor_counts.iter().enumerate() {
                for (axis_idx, count) in axis_counts.iter().enumerate() {
                    if *count > 0 {
                        counts.push((port.as_str(), sensor_idx, axis_idx, *count));
                    }
                }
            }
        }
        counts.sort_by(|a, b| {
            a.0.cmp(b.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.2.cmp(&b.2))
        });
        counts
    }

    fn elapsed_duration(&self) -> Duration {
        if self.status == PlanResultStatus::Running {
            self.started_at
                .map_or(Duration::ZERO, |started| started.elapsed())
        } else {
            self.elapsed
        }
    }

    fn result_label(&self) -> &'static str {
        match self.status {
            PlanResultStatus::Idle => "暂无结果",
            PlanResultStatus::Running => "统计中",
            PlanResultStatus::Interrupted => "已中断",
            PlanResultStatus::Completed if self.alarm_frames == 0 => "通过",
            PlanResultStatus::Completed => "有报警",
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
    standby_timer_message: String,
    standby_timer: Option<StandbyPlanRun>,
    standby_plan_result: PlanResultStats,
    standby_templates: Vec<PlanTemplate>,
    selected_standby_template: usize,
    new_template_name_input: String,
    template_item_label_input: String,
    template_item_hours_input: String,
    template_item_minutes_input: String,
    template_item_seconds_input: String,
    template_item_stage: StandbyTimerStage,
    template_item_repeat_count: usize,
    template_sub_plan_items: Vec<PlanTemplateItem>,
    template_sub_plan_repeat_count: usize,
    template_sub_plan_clear_after_insert: bool,
    timer_page_tab: TimerPageTab,
    template_delete_confirmation: bool,
    plan_queue_expanded: bool,
    template_items_expanded: bool,
    template_edit_message: String,
    invalid_send_message: String,
    log_save_message: String,
    auto_log_path: PathBuf,
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
            standby_timer_message: String::new(),
            standby_timer: None,
            standby_plan_result: PlanResultStats::default(),
            standby_templates: load_plan_templates(),
            selected_standby_template: 0,
            new_template_name_input: "自定义计划".to_string(),
            template_item_label_input: "自定义步骤".to_string(),
            template_item_hours_input: "0".to_string(),
            template_item_minutes_input: "10".to_string(),
            template_item_seconds_input: "0".to_string(),
            template_item_stage: StandbyTimerStage::Normal,
            template_item_repeat_count: 1,
            template_sub_plan_items: Vec::new(),
            template_sub_plan_repeat_count: 1,
            template_sub_plan_clear_after_insert: true,
            timer_page_tab: TimerPageTab::Monitor,
            template_delete_confirmation: false,
            plan_queue_expanded: false,
            template_items_expanded: false,
            template_edit_message: String::new(),
            invalid_send_message: String::new(),
            log_save_message: String::new(),
            auto_log_path: auto_log_path(),
            logs: VecDeque::with_capacity(MAX_LOG_LINES),
        }
    }

    fn push_log(&mut self, line: impl Into<String>) {
        let line = line.into();
        if let Err(err) = self.append_auto_log(&line) {
            self.log_save_message = format!("自动保存日志失败: {err}");
        }

        if self.logs.len() >= MAX_LOG_LINES {
            let _ = self.logs.pop_front();
        }
        self.logs.push_back(line);
    }

    fn append_auto_log(&self, line: &str) -> io::Result<()> {
        fs::create_dir_all(AUTO_LOG_DIR)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.auto_log_path)?;
        writeln!(file, "{line}")
    }

    fn should_count_plan_result_frame(&self) -> bool {
        self.standby_timer
            .as_ref()
            .and_then(|plan| plan.current.as_ref())
            .is_some_and(|event| event.stage == StandbyTimerStage::Normal)
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
                                if self.should_count_plan_result_frame() {
                                    self.standby_plan_result
                                        .record_normal_frame(&port, &parsed.states);
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
                    if !(self.standby_active && should_hide_disconnect_log(&reason)) {
                        self.push_log(format!("{port} 已断开: {reason}"));
                    }
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
        if self.standby_active {
            self.parsers.clear();
            self.latest_frames.clear();
            self.self_test_frames.clear();
            self.self_test_message.clear();
        }
        self.standby_message = format!("{}待机帧已发送到全部串口: {hex}", action.label());
        self.push_log(self.standby_message.clone());
    }

    fn stop_standby_timer(&mut self) {
        if self.standby_timer.take().is_some() {
            self.standby_plan_result
                .finish(PlanResultStatus::Interrupted);
            self.standby_timer_message = "定时切换已停止".to_string();
            self.push_log(self.standby_timer_message.clone());
        }
    }

    fn interrupt_standby_timer(&mut self) {
        if self.standby_timer.take().is_some() {
            self.standby_plan_result
                .finish(PlanResultStatus::Interrupted);
            self.standby_timer_message = "定时计划已中断".to_string();
            self.push_log(self.standby_timer_message.clone());
        } else {
            self.standby_timer_message = "当前没有运行中的定时计划".to_string();
        }
    }

    fn update_standby_timer(&mut self) {
        let should_advance = self
            .standby_timer
            .as_ref()
            .and_then(|plan| plan.current.as_ref())
            .and_then(|event| {
                event
                    .started_at
                    .map(|started| started.elapsed() >= event.duration)
            })
            .unwrap_or(false);

        if !should_advance {
            return;
        }

        if let Some(plan) = self.standby_timer.as_mut() {
            plan.completed_events = plan.completed_events.saturating_add(1);
        }
        self.start_next_plan_event();
    }

    fn start_plan_from_template(&mut self, template: PlanTemplate) {
        if template.items.is_empty() {
            self.standby_timer_message = "计划模板至少需要一个步骤".to_string();
            return;
        }

        self.standby_plan_result.start(template.name.clone());
        self.standby_timer = Some(StandbyPlanRun::from_template(&template));
        self.start_next_plan_event();
    }

    fn append_template_to_current_plan(&mut self, template: PlanTemplate) {
        if template.items.is_empty() {
            self.template_edit_message = "计划模板至少需要一个步骤".to_string();
            return;
        }

        if let Some(plan) = self.standby_timer.as_mut() {
            plan.queue.extend(
                template
                    .items
                    .iter()
                    .map(ScheduledModeEvent::from_template_item),
            );
            self.template_edit_message = format!("已追加模板: {}", template.name);
            self.push_log(self.template_edit_message.clone());
        } else {
            self.start_plan_from_template(template);
        }
    }

    fn start_next_plan_event(&mut self) {
        let next_event = {
            let Some(plan) = self.standby_timer.as_mut() else {
                return;
            };

            let Some(mut event) = plan.queue.pop_front() else {
                let elapsed = plan.started_at.elapsed();
                let completed_events = plan.completed_events;
                let finish_action = plan.finish_action;
                let template_name = plan.template_name.clone();
                self.standby_timer = None;
                self.finish_plan(template_name, completed_events, elapsed, finish_action);
                return;
            };

            event.started_at = Some(Instant::now());
            plan.current = Some(event.clone());
            event
        };

        self.apply_mode_stage(next_event.stage);
        self.standby_timer_message = format!(
            "正在执行: {} / {}，时长 {}",
            next_event.label,
            next_event.stage.label(),
            format_duration(next_event.duration)
        );
        self.push_log(self.standby_timer_message.clone());
    }

    fn finish_plan(
        &mut self,
        template_name: String,
        completed_events: u64,
        elapsed: Duration,
        finish_action: PlanFinishAction,
    ) {
        self.standby_plan_result.finish(PlanResultStatus::Completed);

        match finish_action {
            PlanFinishAction::Normal => self.send_standby_frame(StandbyAction::Stop),
            PlanFinishAction::Standby => self.send_standby_frame(StandbyAction::Start),
            PlanFinishAction::KeepLast => {}
        }

        self.standby_timer_message = format!(
            "计划已完成: {}，完成 {} 个步骤，用时 {}，结束后 {}",
            template_name,
            completed_events,
            format_duration(elapsed),
            finish_action.label()
        );
        self.push_log(self.standby_timer_message.clone());
    }

    fn apply_mode_stage(&mut self, stage: StandbyTimerStage) {
        match stage {
            StandbyTimerStage::Normal => self.send_standby_frame(StandbyAction::Stop),
            StandbyTimerStage::Standby => self.send_standby_frame(StandbyAction::Start),
        }
    }

    fn clamp_selected_template(&mut self) {
        if self.standby_templates.is_empty() {
            self.selected_standby_template = 0;
        } else if self.selected_standby_template >= self.standby_templates.len() {
            self.selected_standby_template = self.standby_templates.len() - 1;
        }
    }

    fn selected_template(&self) -> Option<&PlanTemplate> {
        self.standby_templates.get(self.selected_standby_template)
    }

    fn selected_template_mut(&mut self) -> Option<&mut PlanTemplate> {
        self.standby_templates
            .get_mut(self.selected_standby_template)
    }

    fn start_selected_template(&mut self) {
        let Some(template) = self.selected_template().cloned() else {
            self.template_edit_message = "没有可运行的计划模板".to_string();
            return;
        };

        let template_name = template.name.clone();
        self.start_plan_from_template(template);
        self.template_edit_message = format!("已开始运行: {template_name}");
        self.timer_page_tab = TimerPageTab::Monitor;
    }

    fn append_selected_template(&mut self) {
        let Some(template) = self.selected_template().cloned() else {
            self.template_edit_message = "没有可追加的计划模板".to_string();
            return;
        };

        self.append_template_to_current_plan(template);
    }

    fn create_empty_custom_template(&mut self) {
        let name = self.new_template_name_input.trim();
        if name.is_empty() {
            self.template_edit_message = "模板名称不能为空".to_string();
            return;
        }

        let template = PlanTemplate::new(name, Vec::new(), PlanFinishAction::Normal, true);
        self.standby_templates.push(template);
        self.selected_standby_template = self.standby_templates.len() - 1;
        self.save_custom_templates_with_message(format!("已新建自定义模板: {name}"));
    }

    fn copy_selected_template_as_custom(&mut self) {
        let Some(mut template) = self.selected_template().cloned() else {
            self.template_edit_message = "没有可复制的模板".to_string();
            return;
        };

        let name = self.new_template_name_input.trim();
        template.name = if name.is_empty() {
            format!("{} 副本", template.name)
        } else {
            name.to_string()
        };
        template.is_custom = true;
        self.standby_templates.push(template);
        self.selected_standby_template = self.standby_templates.len() - 1;
        self.save_custom_templates_with_message("已复制为自定义模板".to_string());
    }

    fn save_selected_custom_template(&mut self) {
        let Some(template) = self.selected_template() else {
            self.template_edit_message = "没有可保存的模板".to_string();
            return;
        };

        if !template.is_custom {
            self.template_edit_message = "内置模板不能直接保存，请先复制为自定义模板".to_string();
            return;
        }
        if template.name.trim().is_empty() {
            self.template_edit_message = "模板名称不能为空".to_string();
            return;
        }
        if template.items.is_empty() {
            self.template_edit_message = "模板至少需要一个步骤".to_string();
            return;
        }

        self.save_custom_templates_with_message(format!("已保存自定义模板: {}", template.name));
    }

    fn delete_selected_custom_template(&mut self) {
        let Some(template) = self.selected_template() else {
            self.template_edit_message = "没有可删除的模板".to_string();
            return;
        };
        if !template.is_custom {
            self.template_edit_message = "内置模板不能删除".to_string();
            return;
        }

        let removed_name = template.name.clone();
        self.standby_templates
            .remove(self.selected_standby_template);
        self.clamp_selected_template();
        self.save_custom_templates_with_message(format!("已删除自定义模板: {removed_name}"));
    }

    fn clear_selected_custom_template_items(&mut self) {
        if !self
            .selected_template()
            .is_some_and(|template| template.is_custom)
        {
            self.template_edit_message = "内置模板不能编辑，请先复制为自定义模板".to_string();
            return;
        }

        if let Some(template) = self.selected_template_mut() {
            let removed_count = template.items.len();
            template.items.clear();
            self.template_edit_message = format!("已清空模板步骤: {removed_count} 步");
        }
    }

    fn save_custom_templates_with_message(&mut self, success_message: String) {
        match save_custom_plan_templates(&self.standby_templates) {
            Ok(()) => {
                self.template_edit_message = success_message;
                self.push_log(self.template_edit_message.clone());
            }
            Err(err) => {
                self.template_edit_message = format!("保存自定义模板失败: {err}");
            }
        }
    }

    fn item_from_template_inputs(&mut self) -> Option<PlanTemplateItem> {
        let duration = match parse_hms_duration(
            &self.template_item_hours_input,
            &self.template_item_minutes_input,
            &self.template_item_seconds_input,
        ) {
            Some(duration) => duration,
            None => {
                self.template_edit_message = "步骤时长无效".to_string();
                return None;
            }
        };

        let label = self.template_item_label_input.trim();
        let label = if label.is_empty() {
            self.template_item_stage.label().to_string()
        } else {
            label.to_string()
        };

        Some(PlanTemplateItem::new(
            label,
            self.template_item_stage,
            duration,
        ))
    }

    fn add_item_to_selected_template(&mut self) {
        if !self
            .selected_template()
            .is_some_and(|template| template.is_custom)
        {
            self.template_edit_message = "内置模板不能编辑，请先复制为自定义模板".to_string();
            return;
        }

        let Some(item) = self.item_from_template_inputs() else {
            return;
        };
        let label = item.label.clone();
        let repeat_count = self
            .template_item_repeat_count
            .clamp(1, MAX_TEMPLATE_ITEM_REPEAT_COUNT);
        self.template_item_repeat_count = repeat_count;

        if let Some(template) = self.selected_template_mut() {
            template
                .items
                .extend((0..repeat_count).map(|_| item.clone()));
            self.template_edit_message = if repeat_count == 1 {
                format!("已添加步骤: {label}")
            } else {
                format!("已添加步骤: {label} x {repeat_count}")
            };
        }
    }

    fn add_item_to_template_sub_plan(&mut self) {
        let Some(item) = self.item_from_template_inputs() else {
            return;
        };

        let label = item.label.clone();
        self.template_sub_plan_items.push(item);
        self.template_edit_message = format!(
            "已加入循环步骤组: {label}，当前一轮共 {} 步",
            self.template_sub_plan_items.len()
        );
    }

    fn add_sub_plan_to_selected_template(&mut self) {
        if !self
            .selected_template()
            .is_some_and(|template| template.is_custom)
        {
            self.template_edit_message = "内置模板不能编辑，请先复制为自定义模板".to_string();
            return;
        }

        if self.template_sub_plan_items.is_empty() {
            self.template_edit_message = "循环步骤组至少需要一个步骤".to_string();
            return;
        }

        let repeat_count = self
            .template_sub_plan_repeat_count
            .clamp(1, MAX_TEMPLATE_ITEM_REPEAT_COUNT);
        self.template_sub_plan_repeat_count = repeat_count;
        let sub_plan_items = self.template_sub_plan_items.clone();
        let sub_plan_len = sub_plan_items.len();

        if let Some(template) = self.selected_template_mut() {
            template
                .items
                .extend((0..repeat_count).flat_map(|_| sub_plan_items.iter().cloned()));
            self.template_edit_message =
                format!("已插入循环步骤组: 每轮 {sub_plan_len} 步，共 {repeat_count} 轮");
        }

        if self.template_sub_plan_clear_after_insert {
            self.template_sub_plan_items.clear();
        }
    }

    fn clear_template_sub_plan(&mut self) {
        self.template_sub_plan_items.clear();
        self.template_edit_message = "循环步骤组已清空".to_string();
    }

    fn remove_template_sub_plan_item(&mut self, index: usize) {
        if index < self.template_sub_plan_items.len() {
            let item = self.template_sub_plan_items.remove(index);
            self.template_edit_message = format!("已从循环步骤组删除: {}", item.label);
        }
    }

    fn move_template_sub_plan_item(&mut self, index: usize, offset: isize) {
        let len = self.template_sub_plan_items.len();
        if index >= len {
            return;
        }

        let next_index = index.saturating_add_signed(offset).min(len - 1);
        if next_index != index {
            self.template_sub_plan_items.swap(index, next_index);
        }
    }

    fn remove_template_item(&mut self, index: usize) {
        if !self
            .selected_template()
            .is_some_and(|template| template.is_custom)
        {
            self.template_edit_message = "内置模板不能编辑，请先复制为自定义模板".to_string();
            return;
        }

        if let Some(template) = self.selected_template_mut() {
            if index < template.items.len() {
                let item = template.items.remove(index);
                self.template_edit_message = format!("已删除步骤: {}", item.label);
            }
        }
    }

    fn move_template_item(&mut self, index: usize, offset: isize) {
        if !self
            .selected_template()
            .is_some_and(|template| template.is_custom)
        {
            self.template_edit_message = "内置模板不能编辑，请先复制为自定义模板".to_string();
            return;
        }

        if let Some(template) = self.selected_template_mut() {
            let len = template.items.len();
            if index >= len {
                return;
            }
            let next_index = index.saturating_add_signed(offset).min(len - 1);
            if next_index != index {
                template.items.swap(index, next_index);
            }
        }
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
        if self.standby_active {
            ui.heading("串口状态窗口");
            ui.label("待机模式中");
            return;
        }

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

    fn draw_timer_mode(&mut self, ui: &mut egui::Ui) {
        self.clamp_selected_template();

        ui.horizontal(|ui| {
            ui.heading("定时计划");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let (label, color) = if self.standby_active {
                    ("● 待机模式", egui::Color32::from_rgb(245, 158, 11))
                } else {
                    ("● 正常模式", egui::Color32::from_rgb(38, 166, 91))
                };
                ui.colored_label(color, label);
            });
        });
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            for tab in [TimerPageTab::Monitor, TimerPageTab::Editor] {
                ui.selectable_value(&mut self.timer_page_tab, tab, tab.label());
            }
        });
        ui.separator();

        match self.timer_page_tab {
            TimerPageTab::Monitor => self.draw_timer_monitor(ui),
            TimerPageTab::Editor => self.draw_timer_editor(ui),
        }
    }

    fn draw_timer_monitor(&mut self, ui: &mut egui::Ui) {
        if let Some(plan) = self.standby_timer.clone() {
            self.draw_plan_run_status(ui, &plan);
            ui.add_space(10.0);
            ui.columns(2, |columns| {
                self.draw_plan_queue_panel(&mut columns[0], &plan);
                self.draw_plan_result_panel(&mut columns[1]);
            });
        } else {
            ui.group(|ui| {
                ui.set_min_width(ui.available_width());
                ui.heading("当前没有运行中的计划");
                ui.label("选择一个模板即可开始，运行后可在这里查看倒计时和执行队列。");
                ui.add_space(8.0);
                self.draw_template_selector(ui, "monitor_template_selector");
                let can_start = self
                    .selected_template()
                    .is_some_and(|template| !template.items.is_empty());
                let start_button = egui::Button::new("▶ 立即运行")
                    .fill(ui.visuals().selection.bg_fill)
                    .min_size(egui::vec2(120.0, 34.0));
                if ui.add_enabled(can_start, start_button).clicked() {
                    self.start_selected_template();
                }
                if !can_start {
                    ui.small("所选模板没有步骤，请先到“计划编辑”中添加步骤。");
                }
            });

            if self.standby_plan_result.status != PlanResultStatus::Idle {
                ui.add_space(10.0);
                self.draw_plan_result_panel(ui);
            }
        }

        if !self.standby_timer_message.is_empty() {
            ui.add_space(8.0);
            draw_feedback(ui, &self.standby_timer_message);
        }
    }

    fn draw_timer_editor(&mut self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.set_min_width(ui.available_width());
            ui.heading("模板库");
            self.draw_template_selector(ui, "editor_template_selector");
            ui.add_space(6.0);

            let has_items = self
                .selected_template()
                .is_some_and(|template| !template.items.is_empty());
            let is_custom = self
                .selected_template()
                .is_some_and(|template| template.is_custom);
            ui.horizontal_wrapped(|ui| {
                let run_button =
                    egui::Button::new("▶ 立即运行").fill(ui.visuals().selection.bg_fill);
                if ui.add_enabled(has_items, run_button).clicked() {
                    self.start_selected_template();
                }

                let append = ui
                    .add_enabled(
                        self.standby_timer.is_some() && has_items,
                        egui::Button::new("追加到运行队列"),
                    )
                    .on_hover_text("仅在已有计划运行时可追加");
                if append.clicked() {
                    self.append_selected_template();
                }

                if ui
                    .add_enabled(is_custom, egui::Button::new("保存修改"))
                    .clicked()
                {
                    self.save_selected_custom_template();
                }
            });

            egui::CollapsingHeader::new("新建或复制模板")
                .id_salt("create_or_copy_template")
                .show(ui, |ui| {
                    ui.horizontal_wrapped(|ui| {
                        ui.label("新模板名称");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.new_template_name_input)
                                .desired_width(180.0),
                        );
                        if ui.button("新建空模板").clicked() {
                            self.create_empty_custom_template();
                        }
                        if ui.button("复制所选模板").clicked() {
                            self.copy_selected_template_as_custom();
                        }
                    });
                });
        });

        if self.standby_templates.is_empty() {
            ui.label("暂无模板，请先新建一个自定义模板。");
        } else {
            ui.add_space(10.0);
            self.draw_selected_template_editor(ui);
        }

        if !self.template_edit_message.is_empty() {
            ui.add_space(8.0);
            draw_feedback(ui, &self.template_edit_message);
        }
    }

    fn draw_template_selector(&mut self, ui: &mut egui::Ui, id: &'static str) {
        if self.standby_templates.is_empty() {
            ui.label("暂无可用模板");
            return;
        }

        let selected_name = self
            .selected_template()
            .map(|template| template.name.clone())
            .unwrap_or_else(|| "未选择".to_string());
        let previous_selection = self.selected_standby_template;
        egui::ComboBox::from_id_salt(id)
            .selected_text(selected_name)
            .width(240.0)
            .show_ui(ui, |ui| {
                for (idx, template) in self.standby_templates.iter().enumerate() {
                    let kind = if template.is_custom {
                        "自定义"
                    } else {
                        "内置"
                    };
                    ui.selectable_value(
                        &mut self.selected_standby_template,
                        idx,
                        format!("{} · {kind}", template.name),
                    );
                }
            });
        if self.selected_standby_template != previous_selection {
            self.template_delete_confirmation = false;
        }
    }

    fn draw_plan_result_panel(&self, ui: &mut egui::Ui) {
        let stats = &self.standby_plan_result;

        ui.group(|ui| {
            ui.set_min_width(ui.available_width());
            ui.heading(if stats.status == PlanResultStatus::Running {
                "实时统计"
            } else {
                "最近一次结果"
            });
            ui.horizontal_wrapped(|ui| {
                let color = match stats.status {
                    PlanResultStatus::Completed if stats.alarm_frames == 0 => {
                        egui::Color32::from_rgb(38, 166, 91)
                    }
                    PlanResultStatus::Completed => egui::Color32::from_rgb(220, 53, 69),
                    PlanResultStatus::Interrupted => egui::Color32::from_rgb(245, 158, 11),
                    _ => ui.visuals().text_color(),
                };
                ui.colored_label(color, egui::RichText::new(stats.result_label()).strong());
                ui.label(format!("· {}", stats.status.label()));
            });

            if stats.status == PlanResultStatus::Idle {
                ui.label("计划开始后，这里将显示检测统计。");
                return;
            }

            ui.small(format!("模板：{}", stats.template_name));
            ui.add_space(6.0);
            ui.columns(3, |columns| {
                columns[0].strong(stats.checked_frames.to_string());
                columns[0].small("统计帧数");
                columns[1].strong(stats.alarm_frames.to_string());
                columns[1].small("报警帧数");
                columns[2].strong(stats.alarm_points.to_string());
                columns[2].small("报警点数");
            });
            ui.add_space(6.0);
            ui.label(format!(
                "统计用时：{}",
                format_duration(stats.elapsed_duration())
            ));
            ui.small("仅统计正常模式阶段收到的状态帧");

            if stats.last_alarm.is_empty() {
                ui.label("最后报警：无");
            } else {
                ui.colored_label(
                    egui::Color32::from_rgb(220, 53, 69),
                    format!("最后报警：{}", stats.last_alarm),
                );
            }

            let port_axis_counts = stats.sorted_port_axis_counts();
            if !port_axis_counts.is_empty() {
                egui::CollapsingHeader::new("查看各串口 / 轴位报警次数")
                    .id_salt("plan_alarm_details")
                    .show(ui, |ui| {
                        egui::Grid::new("plan_result_port_axis_counts")
                            .num_columns(3)
                            .spacing(egui::vec2(12.0, 6.0))
                            .striped(true)
                            .show(ui, |ui| {
                                ui.strong("串口");
                                ui.strong("轴位");
                                ui.strong("次数");
                                ui.end_row();

                                for (port, sensor_idx, axis_idx, count) in port_axis_counts {
                                    ui.label(port);
                                    ui.label(format!(
                                        "{}{}",
                                        SENSOR_NAMES[sensor_idx], AXIS_NAMES[axis_idx]
                                    ));
                                    ui.label(count.to_string());
                                    ui.end_row();
                                }
                            });
                    });
            }
        });
    }

    fn draw_plan_run_status(&mut self, ui: &mut egui::Ui, plan: &StandbyPlanRun) {
        ui.group(|ui| {
            ui.set_min_width(ui.available_width());
            ui.horizontal(|ui| {
                ui.heading(&plan.template_name);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(format!(
                        "已运行 {}",
                        format_duration(plan.started_at.elapsed())
                    ));
                });
            });

            let total_steps = plan.completed_events as usize
                + plan.queue.len()
                + usize::from(plan.current.is_some());
            let completed = plan.completed_events;

            if let Some(current) = &plan.current {
                let elapsed = current
                    .started_at
                    .map_or(Duration::ZERO, |started| started.elapsed());
                let progress = if current.duration.is_zero() {
                    1.0
                } else {
                    (elapsed.as_secs_f32() / current.duration.as_secs_f32()).clamp(0.0, 1.0)
                };
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    draw_stage_badge(ui, current.stage);
                    ui.strong(&current.label);
                });
                ui.label(
                    egui::RichText::new(format_duration(duration_remaining(
                        current.duration,
                        elapsed,
                    )))
                    .size(30.0)
                    .strong(),
                );
                ui.add(
                    egui::ProgressBar::new(progress)
                        .desired_width(ui.available_width())
                        .text(format!("当前步骤 {:.0}%", progress * 100.0)),
                );
            } else {
                ui.label("正在准备下一步骤…");
            }

            ui.add_space(6.0);
            ui.horizontal_wrapped(|ui| {
                ui.label(format!("已完成 {completed}/{total_steps} 步"));
                ui.separator();
                ui.label(format!(
                    "总剩余 {}",
                    format_duration(plan.pending_duration())
                ));
                ui.separator();
                ui.label(format!("结束后：{}", plan.finish_action.label()));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let stop =
                        egui::Button::new("中断计划").fill(egui::Color32::from_rgb(180, 55, 60));
                    if ui.add(stop).clicked() {
                        self.interrupt_standby_timer();
                    }
                });
            });

            if let Some(next) = plan.queue.front() {
                ui.small(format!(
                    "下一步：{} · {} · {}",
                    next.label,
                    next.stage.label(),
                    format_duration(next.duration)
                ));
            }
        });
    }

    fn draw_plan_queue_panel(&mut self, ui: &mut egui::Ui, plan: &StandbyPlanRun) {
        ui.group(|ui| {
            ui.set_min_width(ui.available_width());
            ui.horizontal(|ui| {
                ui.heading("执行队列");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.small(format!("等待 {} 步", plan.queue.len()));
                });
            });

            if let Some(current) = &plan.current {
                ui.horizontal(|ui| {
                    ui.colored_label(egui::Color32::from_rgb(59, 130, 246), "▶");
                    ui.strong(&current.label);
                    draw_stage_badge(ui, current.stage);
                });
            }

            let visible_count = if self.plan_queue_expanded {
                plan.queue.len()
            } else {
                4
            };
            for event in plan.queue.iter().take(visible_count) {
                ui.horizontal(|ui| {
                    ui.label("○");
                    ui.label(&event.label);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.small(format_duration(event.duration));
                        ui.small(event.stage.label());
                    });
                });
            }

            if plan.queue.is_empty() {
                ui.small("没有等待中的步骤");
            } else if plan.queue.len() > 4 {
                let hidden = plan.queue.len().saturating_sub(4);
                let label = if self.plan_queue_expanded {
                    "收起队列".to_string()
                } else {
                    format!("展开其余 {hidden} 步")
                };
                if ui.small_button(label).clicked() {
                    self.plan_queue_expanded = !self.plan_queue_expanded;
                }
            }
        });
    }

    fn draw_selected_template_editor(&mut self, ui: &mut egui::Ui) {
        let Some(template) = self.selected_template() else {
            return;
        };
        let is_custom = template.is_custom;
        let items_snapshot = template.items.clone();
        let finish_action = template.finish_action;
        let total_duration = items_snapshot
            .iter()
            .fold(Duration::ZERO, |total, item| total + item.duration);

        ui.group(|ui| {
            ui.set_min_width(ui.available_width());
            ui.horizontal(|ui| {
                ui.heading("模板设置");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.small(if is_custom {
                        "自定义模板"
                    } else {
                        "内置模板 · 只读"
                    });
                });
            });

            if is_custom {
                if let Some(template) = self.selected_template_mut() {
                    ui.horizontal_wrapped(|ui| {
                        ui.label("模板名称");
                        ui.add(egui::TextEdit::singleline(&mut template.name).desired_width(190.0));
                        ui.label("结束后");
                        egui::ComboBox::from_id_salt("template_finish_action")
                            .selected_text(template.finish_action.label())
                            .show_ui(ui, |ui| {
                                ui.selectable_value(
                                    &mut template.finish_action,
                                    PlanFinishAction::Normal,
                                    PlanFinishAction::Normal.label(),
                                );
                                ui.selectable_value(
                                    &mut template.finish_action,
                                    PlanFinishAction::Standby,
                                    PlanFinishAction::Standby.label(),
                                );
                                ui.selectable_value(
                                    &mut template.finish_action,
                                    PlanFinishAction::KeepLast,
                                    PlanFinishAction::KeepLast.label(),
                                );
                            });
                    });
                }

                ui.horizontal_wrapped(|ui| {
                    if !self.template_delete_confirmation {
                        if ui
                            .add(
                                egui::Button::new("删除模板")
                                    .fill(egui::Color32::from_rgb(125, 45, 50)),
                            )
                            .clicked()
                        {
                            self.template_delete_confirmation = true;
                        }
                    } else {
                        ui.colored_label(
                            egui::Color32::from_rgb(220, 53, 69),
                            "确定删除这个模板？",
                        );
                        if ui.button("确认删除").clicked() {
                            self.delete_selected_custom_template();
                            self.template_delete_confirmation = false;
                        }
                        if ui.button("取消").clicked() {
                            self.template_delete_confirmation = false;
                        }
                    }
                });
            } else {
                ui.label("内置模板不能直接编辑，可在上方复制为自定义模板。");
                ui.label(format!("结束后：{}", finish_action.label()));
            }
        });

        ui.add_space(10.0);
        ui.group(|ui| {
            ui.set_min_width(ui.available_width());
            ui.horizontal(|ui| {
                ui.heading("执行步骤");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.small(format!(
                        "共 {} 步 · 总时长 {}",
                        items_snapshot.len(),
                        format_duration(total_duration)
                    ));
                });
            });

            if items_snapshot.is_empty() {
                ui.label("模板还没有步骤，请在下方添加第一个步骤。");
            } else {
                let visible_count = if self.template_items_expanded {
                    items_snapshot.len()
                } else {
                    items_snapshot.len().min(6)
                };
                egui::Grid::new("template_items_grid")
                    .num_columns(6)
                    .spacing(egui::vec2(12.0, 7.0))
                    .striped(true)
                    .show(ui, |ui| {
                        ui.strong("#");
                        ui.strong("步骤名称");
                        ui.strong("模式");
                        ui.strong("时长");
                        ui.strong("排序");
                        ui.strong("操作");
                        ui.end_row();

                        for (idx, item) in items_snapshot.iter().take(visible_count).enumerate() {
                            ui.label((idx + 1).to_string());
                            ui.label(&item.label);
                            draw_stage_badge(ui, item.stage);
                            ui.label(format_duration(item.duration));
                            if is_custom {
                                ui.horizontal(|ui| {
                                    if ui.small_button("↑").clicked() {
                                        self.move_template_item(idx, -1);
                                    }
                                    if ui.small_button("↓").clicked() {
                                        self.move_template_item(idx, 1);
                                    }
                                });
                                if ui.small_button("删除").clicked() {
                                    self.remove_template_item(idx);
                                }
                            } else {
                                ui.label("—");
                                ui.label("—");
                            }
                            ui.end_row();
                        }
                    });

                if items_snapshot.len() > 6 {
                    let label = if self.template_items_expanded {
                        "收起步骤"
                    } else {
                        "展开全部步骤"
                    };
                    if ui.small_button(label).clicked() {
                        self.template_items_expanded = !self.template_items_expanded;
                    }
                }
            }

            if is_custom && !items_snapshot.is_empty() {
                ui.separator();
                if ui.button("清空全部步骤").clicked() {
                    self.clear_selected_custom_template_items();
                }
            }
        });

        if !is_custom {
            return;
        }

        ui.add_space(10.0);
        ui.group(|ui| {
            ui.set_min_width(ui.available_width());
            ui.heading("新步骤");
            ui.small("设置一次步骤内容，再选择添加到主计划，或暂存到下方的循环步骤组。");
            ui.add_space(6.0);
            ui.horizontal_wrapped(|ui| {
                ui.label("模式");
                egui::ComboBox::from_id_salt("template_item_stage")
                    .selected_text(self.template_item_stage.label())
                    .show_ui(ui, |ui| {
                        ui.selectable_value(
                            &mut self.template_item_stage,
                            StandbyTimerStage::Normal,
                            StandbyTimerStage::Normal.label(),
                        );
                        ui.selectable_value(
                            &mut self.template_item_stage,
                            StandbyTimerStage::Standby,
                            StandbyTimerStage::Standby.label(),
                        );
                    });
                ui.label("名称");
                ui.add(
                    egui::TextEdit::singleline(&mut self.template_item_label_input)
                        .desired_width(130.0),
                );
                ui.label("时长");
                ui.add(
                    egui::TextEdit::singleline(&mut self.template_item_hours_input)
                        .desired_width(34.0),
                );
                ui.label("时");
                ui.add(
                    egui::TextEdit::singleline(&mut self.template_item_minutes_input)
                        .desired_width(34.0),
                );
                ui.label("分");
                ui.add(
                    egui::TextEdit::singleline(&mut self.template_item_seconds_input)
                        .desired_width(34.0),
                );
                ui.label("秒");
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("添加到主计划时重复");
                ui.add(
                    egui::DragValue::new(&mut self.template_item_repeat_count)
                        .range(1..=MAX_TEMPLATE_ITEM_REPEAT_COUNT)
                        .suffix(" 次")
                        .speed(1),
                );
                if ui.button("＋ 添加到主计划").clicked() {
                    self.add_item_to_selected_template();
                }
                if ui.button("加入循环步骤组 ↓").clicked() {
                    self.add_item_to_template_sub_plan();
                }
            });
        });

        ui.add_space(10.0);
        self.draw_loop_group_editor(ui);
    }

    fn draw_loop_group_editor(&mut self, ui: &mut egui::Ui) {
        let items = self.template_sub_plan_items.clone();
        let round_duration = items
            .iter()
            .fold(Duration::ZERO, |total, item| total + item.duration);
        let repeat_count = self
            .template_sub_plan_repeat_count
            .clamp(1, MAX_TEMPLATE_ITEM_REPEAT_COUNT);
        let generated_steps = items.len().saturating_mul(repeat_count);
        let generated_duration = round_duration
            .checked_mul(repeat_count as u32)
            .unwrap_or(Duration::MAX);

        ui.group(|ui| {
            ui.set_min_width(ui.available_width());
            ui.horizontal(|ui| {
                ui.heading("循环步骤组");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.small("高级");
                });
            });
            ui.label("适合“正常 10 分钟 → 待机 20 分钟”这类需要重复多轮的流程。");
            ui.small("先用上方按钮组装一轮，确认顺序后设置循环次数；插入时会展开为普通步骤。");

            ui.add_space(8.0);
            ui.horizontal_wrapped(|ui| {
                ui.strong(format!("一轮 {} 步", items.len()));
                ui.separator();
                ui.label(format!("一轮时长 {}", format_duration(round_duration)));
                ui.separator();
                ui.label(format!("循环 {repeat_count} 次"));
                ui.separator();
                ui.strong(format!(
                    "最终生成 {generated_steps} 步 / {}",
                    format_duration(generated_duration)
                ));
            });

            if items.is_empty() {
                ui.add_space(8.0);
                ui.label("循环组为空，请先在上方设置步骤，然后点击“加入循环步骤组”。");
            } else {
                ui.add_space(8.0);
                egui::Grid::new("template_loop_group_grid")
                    .num_columns(6)
                    .spacing(egui::vec2(12.0, 7.0))
                    .striped(true)
                    .show(ui, |ui| {
                        ui.strong("#");
                        ui.strong("步骤名称");
                        ui.strong("模式");
                        ui.strong("时长");
                        ui.strong("排序");
                        ui.strong("操作");
                        ui.end_row();

                        for (idx, item) in items.iter().enumerate() {
                            ui.label((idx + 1).to_string());
                            ui.label(&item.label);
                            draw_stage_badge(ui, item.stage);
                            ui.label(format_duration(item.duration));
                            ui.horizontal(|ui| {
                                if ui.small_button("↑").clicked() {
                                    self.move_template_sub_plan_item(idx, -1);
                                }
                                if ui.small_button("↓").clicked() {
                                    self.move_template_sub_plan_item(idx, 1);
                                }
                            });
                            if ui.small_button("移除").clicked() {
                                self.remove_template_sub_plan_item(idx);
                            }
                            ui.end_row();
                        }
                    });
            }

            ui.add_space(8.0);
            ui.horizontal_wrapped(|ui| {
                ui.label("循环次数");
                ui.add(
                    egui::DragValue::new(&mut self.template_sub_plan_repeat_count)
                        .range(1..=MAX_TEMPLATE_ITEM_REPEAT_COUNT)
                        .suffix(" 次")
                        .speed(1),
                );
                ui.checkbox(
                    &mut self.template_sub_plan_clear_after_insert,
                    "插入后清空循环组",
                );
            });
            ui.horizontal_wrapped(|ui| {
                let insert_button =
                    egui::Button::new(format!("插入主计划（生成 {generated_steps} 步）"))
                        .fill(ui.visuals().selection.bg_fill);
                if ui.add_enabled(!items.is_empty(), insert_button).clicked() {
                    self.add_sub_plan_to_selected_template();
                }
                if ui
                    .add_enabled(!items.is_empty(), egui::Button::new("清空循环组"))
                    .clicked()
                {
                    self.clear_template_sub_plan();
                }
            });
        });
    }

    fn save_logs_to_file(&mut self) {
        if self.logs.is_empty() {
            self.log_save_message = "没有可保存的日志".to_string();
            return;
        }

        let dir = Path::new(CURRENT_LOG_DIR);
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
        self.log_save_message = "当前界面日志已清除，自动保存文件不受影响".to_string();
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
        ui.label(format!("自动保存: {}", self.auto_log_path.display()));
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
        self.update_standby_timer();

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
                        self.stop_standby_timer();
                        self.send_standby_frame(StandbyAction::Start);
                    }
                    if ui.button("停止待机").clicked() {
                        self.stop_standby_timer();
                        self.send_standby_frame(StandbyAction::Stop);
                    }
                });
                if !self.standby_message.is_empty() {
                    ui.label(&self.standby_message);
                }
            });

        egui::TopBottomPanel::bottom("logs_panel")
            .resizable(true)
            .default_height(220.0)
            .min_height(120.0)
            .show(ctx, |ui| {
                self.draw_logs(ui);
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical()
                .id_salt("main_content_scroll")
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    // Keep the plan controls in the visible central area. A pair of
                    // unconstrained horizontal columns lets the overview consume the
                    // available width and pushes the template controls off-screen.
                    self.draw_timer_mode(ui);
                    ui.add_space(12.0);
                    ui.separator();
                    self.draw_overview(ui);
                    self.draw_self_test_overview(ui);
                });
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

fn draw_stage_badge(ui: &mut egui::Ui, stage: StandbyTimerStage) {
    let color = match stage {
        StandbyTimerStage::Normal => egui::Color32::from_rgb(38, 166, 91),
        StandbyTimerStage::Standby => egui::Color32::from_rgb(245, 158, 11),
    };
    ui.label(
        egui::RichText::new(stage.label())
            .color(color)
            .background_color(color.gamma_multiply(0.14)),
    );
}

fn draw_feedback(ui: &mut egui::Ui, message: &str) {
    let is_error = ["失败", "不能", "不能为空", "无效", "至少", "没有可"]
        .iter()
        .any(|keyword| message.contains(keyword));
    let color = if is_error {
        egui::Color32::from_rgb(220, 53, 69)
    } else {
        egui::Color32::from_rgb(38, 166, 91)
    };
    let prefix = if is_error { "!" } else { "✓" };
    ui.colored_label(color, format!("{prefix} {message}"));
}

fn count_alarm_points(states: &[[AlarmState; 3]; 3]) -> u64 {
    states
        .iter()
        .flat_map(|sensor| sensor.iter())
        .filter(|state| **state == AlarmState::Alarm)
        .count() as u64
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

fn should_hide_disconnect_log(reason: &str) -> bool {
    reason.contains("open failed")
        && (reason.contains("拒绝访问") || reason.contains("Access denied"))
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

fn auto_log_path() -> PathBuf {
    Path::new(AUTO_LOG_DIR).join(format!("auto_log_{}.txt", filename_time_string()))
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

fn parse_nonnegative_u64(input: &str) -> Option<u64> {
    input.trim().parse::<u64>().ok()
}

fn parse_hms_duration(hours: &str, minutes: &str, seconds: &str) -> Option<Duration> {
    let hours = parse_nonnegative_u64(hours)?;
    let minutes = parse_nonnegative_u64(minutes)?;
    let seconds = parse_nonnegative_u64(seconds)?;
    if minutes >= 60 || seconds >= 60 {
        return None;
    }

    let total_seconds = hours
        .checked_mul(3600)?
        .checked_add(minutes.checked_mul(60)?)?
        .checked_add(seconds)?;

    if total_seconds == 0 {
        None
    } else {
        Some(Duration::from_secs(total_seconds))
    }
}

fn load_plan_templates() -> Vec<PlanTemplate> {
    let mut templates = default_plan_templates();
    templates.extend(load_custom_plan_templates());
    templates
}

fn default_plan_templates() -> Vec<PlanTemplate> {
    vec![
        template_from_interval_cycles("默认模板", Duration::from_secs(30 * 60), 3),
        PlanTemplate::new(
            "长时间运行",
            vec![
                PlanTemplateItem::new(
                    "长运行",
                    StandbyTimerStage::Normal,
                    Duration::from_secs(120 * 60),
                ),
                PlanTemplateItem::new(
                    "短待机",
                    StandbyTimerStage::Standby,
                    Duration::from_secs(10 * 60),
                ),
            ],
            PlanFinishAction::Normal,
            false,
        ),
    ]
}

fn template_from_interval_cycles(
    name: impl Into<String>,
    interval: Duration,
    cycles: u32,
) -> PlanTemplate {
    let mut items = Vec::with_capacity(cycles as usize * 2);
    for cycle in 1..=cycles {
        items.push(PlanTemplateItem::new(
            format!("第 {cycle} 次正常"),
            StandbyTimerStage::Normal,
            interval,
        ));
        items.push(PlanTemplateItem::new(
            format!("第 {cycle} 次待机"),
            StandbyTimerStage::Standby,
            interval,
        ));
    }

    PlanTemplate::new(name, items, PlanFinishAction::Normal, false)
}

fn load_custom_plan_templates() -> Vec<PlanTemplate> {
    let Ok(content) = fs::read_to_string(CUSTOM_PLAN_TEMPLATES_PATH) else {
        return Vec::new();
    };

    let mut templates = Vec::new();
    let mut current: Option<PlanTemplate> = None;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts = line.split('\t').collect::<Vec<_>>();
        match parts.as_slice() {
            ["template", name, finish_action] => {
                if let Some(template) = current.take() {
                    if !template.items.is_empty() {
                        templates.push(template);
                    }
                }
                current = Some(PlanTemplate::new(
                    decode_template_field(name),
                    Vec::new(),
                    parse_finish_action(finish_action).unwrap_or(PlanFinishAction::Normal),
                    true,
                ));
            }
            ["template", name, _legacy_flag, finish_action] => {
                if let Some(template) = current.take() {
                    if !template.items.is_empty() {
                        templates.push(template);
                    }
                }
                current = Some(PlanTemplate::new(
                    decode_template_field(name),
                    Vec::new(),
                    parse_finish_action(finish_action).unwrap_or(PlanFinishAction::Normal),
                    true,
                ));
            }
            ["item", label, stage, seconds] => {
                let Some(template) = current.as_mut() else {
                    continue;
                };
                let Some(stage) = parse_timer_stage(stage) else {
                    continue;
                };
                let Ok(seconds) = seconds.parse::<u64>() else {
                    continue;
                };
                if seconds == 0 {
                    continue;
                }
                template.items.push(PlanTemplateItem::new(
                    decode_template_field(label),
                    stage,
                    Duration::from_secs(seconds),
                ));
            }
            ["end"] => {
                if let Some(template) = current.take() {
                    if !template.items.is_empty() {
                        templates.push(template);
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(template) = current {
        if !template.items.is_empty() {
            templates.push(template);
        }
    }

    templates
}

fn save_custom_plan_templates(templates: &[PlanTemplate]) -> io::Result<()> {
    let mut lines = Vec::new();
    for template in templates.iter().filter(|template| template.is_custom) {
        lines.push(format!(
            "template\t{}\t{}",
            encode_template_field(&template.name),
            finish_action_code(template.finish_action)
        ));
        for item in &template.items {
            lines.push(format!(
                "item\t{}\t{}\t{}",
                encode_template_field(&item.label),
                timer_stage_code(item.stage),
                item.duration.as_secs()
            ));
        }
        lines.push("end".to_string());
    }

    fs::write(CUSTOM_PLAN_TEMPLATES_PATH, lines.join("\n"))
}

fn encode_template_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn decode_template_field(value: &str) -> String {
    let mut decoded = String::new();
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }

        match chars.next() {
            Some('t') => decoded.push('\t'),
            Some('n') => decoded.push('\n'),
            Some('r') => decoded.push('\r'),
            Some('\\') => decoded.push('\\'),
            Some(other) => {
                decoded.push('\\');
                decoded.push(other);
            }
            None => decoded.push('\\'),
        }
    }
    decoded
}

fn finish_action_code(action: PlanFinishAction) -> &'static str {
    match action {
        PlanFinishAction::Normal => "normal",
        PlanFinishAction::Standby => "standby",
        PlanFinishAction::KeepLast => "keep-last",
    }
}

fn parse_finish_action(value: &str) -> Option<PlanFinishAction> {
    match value {
        "normal" => Some(PlanFinishAction::Normal),
        "standby" => Some(PlanFinishAction::Standby),
        "keep-last" => Some(PlanFinishAction::KeepLast),
        _ => None,
    }
}

fn timer_stage_code(stage: StandbyTimerStage) -> &'static str {
    match stage {
        StandbyTimerStage::Normal => "normal",
        StandbyTimerStage::Standby => "standby",
    }
}

fn parse_timer_stage(value: &str) -> Option<StandbyTimerStage> {
    match value {
        "normal" => Some(StandbyTimerStage::Normal),
        "standby" => Some(StandbyTimerStage::Standby),
        _ => None,
    }
}

fn duration_remaining(total: Duration, elapsed: Duration) -> Duration {
    total.checked_sub(elapsed).unwrap_or(Duration::ZERO)
}

fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    if hours > 0 {
        format!("{hours}小时{minutes:02}分{seconds:02}秒")
    } else if minutes > 0 {
        format!("{minutes}分{seconds:02}秒")
    } else {
        format!("{seconds}秒")
    }
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

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 820.0])
            .with_min_inner_size([960.0, 640.0]),
        ..Default::default()
    };
    let app_creator = move |cc: &eframe::CreationContext<'_>| -> Result<
        Box<dyn eframe::App>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        setup_chinese_fonts(&cc.egui_ctx);
        let mut style = (*cc.egui_ctx.style()).clone();
        style.spacing.item_spacing = egui::vec2(8.0, 8.0);
        style.spacing.button_padding = egui::vec2(10.0, 5.0);
        cc.egui_ctx.set_style(style);
        Ok(Box::new(SerialPanelApp::new(manager.clone(), event_rx)))
    };

    eframe::run_native("serial_alarm_panel", options, Box::new(app_creator))
}
