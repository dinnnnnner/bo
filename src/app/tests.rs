use super::*;
use eframe::App;
use std::sync::atomic::{AtomicU64, Ordering};

const NORMAL: [[AlarmState; 3]; 3] = [[AlarmState::Normal; 3]; 3];

struct TestApp {
    app: SerialPanelApp,
    events: mpsc::Sender<PortEvent>,
    log_dir: PathBuf,
}

impl TestApp {
    fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let log_dir = std::env::temp_dir().join(format!(
            "boo-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let (events, rx) = mpsc::channel();
        let mut app = SerialPanelApp::new(SerialManager::new(), rx);
        app.auto_log_path = log_dir.join("events.txt");
        app.standby_templates = default_plan_templates();
        Self {
            app,
            events,
            log_dir,
        }
    }

    fn advance(&mut self) {
        self.app
            .standby_timer
            .as_mut()
            .unwrap()
            .current
            .as_mut()
            .unwrap()
            .started_at = Some(Instant::now() - Duration::from_secs(2));
        self.app.update_standby_timer();
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.log_dir.join("events.txt"));
        let _ = fs::remove_dir(&self.log_dir);
    }
}

fn template(stages: &[StandbyTimerStage]) -> PlanTemplate {
    PlanTemplate::new(
        "test",
        stages
            .iter()
            .map(|stage| PlanTemplateItem::new("step", *stage, Duration::from_secs(1)))
            .collect(),
        PlanFinishAction::KeepLast,
        true,
    )
}

fn stats(ports: &[&str]) -> PlanResultStats {
    let mut stats = PlanResultStats::default();
    stats.start("test", &ports.iter().map(|port| port.to_string()).collect());
    stats.begin_normal_stage();
    stats
}

#[test]
fn empty_and_standby_only_plans_do_not_pass() {
    for stage in [StandbyTimerStage::Normal, StandbyTimerStage::Standby] {
        let mut fixture = TestApp::new();
        fixture.app.start_plan_from_template(template(&[stage]));
        fixture.advance();
        assert_eq!(fixture.app.standby_plan_result.result_label(), "无有效数据");
        assert!(!fixture.app.standby_plan_result.passed());
    }
}

#[test]
fn every_port_must_report_in_every_normal_stage() {
    let mut stats = stats(&["COM1", "COM2"]);
    stats.record_normal_frame("COM1", &NORMAL);
    stats.record_normal_frame("COM2", &NORMAL);
    stats.finish_normal_stage();
    stats.begin_normal_stage();
    stats.record_normal_frame("COM1", &NORMAL);
    stats.finish(PlanResultStatus::Completed);
    assert_eq!(stats.result_label(), "检测不完整");
    assert!(!stats.passed());
}

#[test]
fn fully_observed_normal_stages_pass() {
    let mut stats = stats(&["COM1", "COM2"]);
    for _ in 0..2 {
        stats.record_normal_frame("COM1", &NORMAL);
        stats.record_normal_frame("COM2", &NORMAL);
        stats.finish_normal_stage();
        stats.begin_normal_stage();
    }
    stats.record_normal_frame("COM1", &NORMAL);
    stats.record_normal_frame("COM2", &NORMAL);
    stats.finish(PlanResultStatus::Completed);
    assert_eq!(stats.result_label(), "通过");
    assert!(stats.passed());
}

#[test]
fn plan_transition_detects_an_empty_normal_stage() {
    let mut fixture = TestApp::new();
    fixture.app.connected_ports.insert("COM1".to_string());
    fixture.app.start_plan_from_template(template(&[
        StandbyTimerStage::Normal,
        StandbyTimerStage::Standby,
        StandbyTimerStage::Normal,
    ]));
    fixture
        .app
        .standby_plan_result
        .record_normal_frame("COM1", &NORMAL);
    fixture.advance();
    fixture.advance();
    fixture.advance();
    assert_eq!(fixture.app.standby_plan_result.result_label(), "检测不完整");
}

#[test]
fn invalid_protocol_states_prevent_a_pass_even_after_valid_data() {
    let bytes = [0xAA, 0x55, 0x81, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0x55, 0xAA];
    let parsed = FrameParser::default().push(&bytes);
    let mut stats = stats(&["COM1"]);
    stats.record_normal_frame("COM1", &parsed[0].states);
    stats.record_normal_frame("COM1", &NORMAL);
    stats.finish(PlanResultStatus::Completed);
    assert_eq!(stats.checked_frames, 2);
    assert_eq!(stats.invalid_frames, 1);
    assert_eq!(stats.result_label(), "数据无效");
    assert!(!stats.passed());
}

#[test]
fn invalid_frames_still_preserve_known_alarms() {
    let mut states = NORMAL;
    states[0][0] = AlarmState::Invalid(2);
    states[1][2] = AlarmState::Alarm;
    let mut stats = stats(&["COM1"]);
    stats.record_normal_frame("COM1", &states);
    stats.finish(PlanResultStatus::Completed);
    assert_eq!(stats.invalid_frames, 1);
    assert_eq!(stats.alarm_points, 1);
    assert_eq!(stats.port_axis_alarm_counts["COM1"][1][2], 1);
    assert_eq!(stats.result_label(), "有报警");
}

#[test]
fn a_disconnect_remains_incomplete_after_recovery() {
    let mut stats = stats(&["COM1"]);
    stats.record_normal_frame("COM1", &NORMAL);
    stats.port_disconnected("COM1");
    stats.port_connected("COM1");
    stats.record_normal_frame("COM1", &NORMAL);
    stats.finish(PlanResultStatus::Completed);
    assert_eq!(stats.result_label(), "检测不完整");
}

#[test]
fn late_connections_are_included_in_stage_coverage() {
    let mut stats = stats(&["COM1"]);
    stats.record_normal_frame("COM1", &NORMAL);
    stats.port_connected("COM2");
    stats.finish(PlanResultStatus::Completed);
    assert_eq!(stats.result_label(), "检测不完整");
}

#[test]
fn late_and_reconnected_ports_receive_the_current_mode() {
    for (standby, expected) in [(false, STANDBY_STOP_FRAME), (true, STANDBY_START_FRAME)] {
        let mut fixture = TestApp::new();
        fixture.app.standby_active = standby;
        let manager = fixture.app.manager.clone();
        let commands = manager.add_test_port("COM1");
        for _ in 0..2 {
            fixture
                .events
                .send(PortEvent::Connected("COM1".to_string()))
                .unwrap();
            fixture.app.drain_serial_events();
            assert_eq!(commands(), vec![expected.to_vec()]);
            fixture
                .events
                .send(PortEvent::Disconnected {
                    port: "COM1".to_string(),
                    reason: "test disconnect".to_string(),
                })
                .unwrap();
            fixture.app.drain_serial_events();
        }
    }
}

#[test]
fn sync_enqueue_failure_is_visible_and_invalidates_normal_stage() {
    let mut fixture = TestApp::new();
    fixture
        .app
        .start_plan_from_template(template(&[StandbyTimerStage::Normal]));
    fixture
        .events
        .send(PortEvent::Connected("COM1".to_string()))
        .unwrap();
    fixture.app.drain_serial_events();
    assert!(
        fixture
            .app
            .logs
            .iter()
            .any(|line| line.contains("模式同步失败"))
    );
    assert!(fixture.app.standby_plan_result.incomplete);
}

#[test]
fn sidebar_scroll_reaches_standby_controls_with_feedback() {
    for size in [egui::vec2(960.0, 640.0), egui::vec2(1280.0, 820.0)] {
        let mut fixture = TestApp::new();
        fixture.app.threshold_message = "阈值帧已发送到全部串口".repeat(4);
        fixture.app.self_test_message = "自检结果".repeat(8);
        fixture.app.vibration_message = "振动帧已发送到全部串口".repeat(4);
        fixture.app.environment_message = "环境帧已发送到全部串口".repeat(4);
        fixture.app.standby_message = "待机帧已发送到全部串口".repeat(4);
        let ctx = egui::Context::default();
        setup_chinese_fonts(&ctx);
        let mut style = (*ctx.style()).clone();
        style.spacing.item_spacing = egui::vec2(8.0, 8.0);
        style.spacing.button_padding = egui::vec2(10.0, 5.0);
        ctx.set_style(style);
        let mut frame = eframe::Frame::_new_kittest();
        let mut reached_standby = false;
        for iteration in 0..20 {
            let mut input = egui::RawInput {
                screen_rect: Some(egui::Rect::from_min_size(egui::Pos2::ZERO, size)),
                time: Some(iteration as f64 * 0.1),
                ..Default::default()
            };
            if iteration > 2 {
                input
                    .events
                    .push(egui::Event::PointerMoved(egui::pos2(150.0, 300.0)));
                input.events.push(egui::Event::MouseWheel {
                    unit: egui::MouseWheelUnit::Point,
                    delta: egui::vec2(0.0, -200.0),
                    modifiers: egui::Modifiers::default(),
                });
            }
            let output = ctx.run(input, |ctx| fixture.app.update(ctx, &mut frame));
            let visible_text: Vec<_> = output
                .shapes
                .iter()
                .filter_map(|clipped| {
                    if let egui::Shape::Text(text) = &clipped.shape {
                        let rect = text.galley.rect.translate(text.pos.to_vec2());
                        if rect.left() < 330.0 && clipped.clip_rect.contains_rect(rect) {
                            return Some((text.galley.job.text.as_str(), rect.top()));
                        }
                    }
                    None
                })
                .collect();
            if let Some((_, title_y)) = visible_text
                .iter()
                .find(|(text, _)| *text == "待机开始/停止")
            {
                reached_standby |= visible_text
                    .iter()
                    .any(|(text, y)| *text == "停止" && y > title_y);
            }
        }
        assert!(
            reached_standby,
            "standby stop button unreachable at {size:?}"
        );
    }
}
