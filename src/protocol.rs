use eframe::egui;
use std::time::Instant;

const FRAME_LEN: usize = 14;
const FRAME_PREFIX: [u8; 2] = [0xAA, 0x55];
const STATUS_FRAME_TYPE: u8 = 0x81;
const SELF_TEST_RESULT_FRAME_TYPE: u8 = 0x83;
const FRAME_TAIL: [u8; 2] = [0x55, 0xAA];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AlarmState {
    #[default]
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

    pub fn label(self) -> String {
        match self {
            Self::Normal => "正常".to_string(),
            Self::Alarm => "报警".to_string(),
            Self::Invalid(value) => format!("无效 0x{value:02X}"),
        }
    }

    pub fn color(self) -> egui::Color32 {
        match self {
            Self::Normal => egui::Color32::from_rgb(38, 166, 91),
            Self::Alarm => egui::Color32::from_rgb(220, 53, 69),
            Self::Invalid(_) => egui::Color32::from_rgb(245, 158, 11),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SensorFrame {
    pub port: String,
    pub received_at: Instant,
    pub count: u64,
    pub states: [[AlarmState; 3]; 3],
    pub raw_hex: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParsedFrameKind {
    Status,
    SelfTestResult,
}

#[derive(Debug)]
pub struct ParsedFrame {
    pub kind: ParsedFrameKind,
    pub states: [[AlarmState; 3]; 3],
    pub raw: Vec<u8>,
}

#[derive(Default)]
pub struct FrameParser {
    buffer: Vec<u8>,
}

impl FrameParser {
    pub fn push(&mut self, data: &[u8]) -> Vec<ParsedFrame> {
        self.buffer.extend_from_slice(data);
        let mut frames = Vec::new();

        loop {
            let Some((pos, kind)) = find_frame_header(&self.buffer) else {
                if self.buffer.len() > FRAME_PREFIX.len() {
                    let drop_len = self.buffer.len() - FRAME_PREFIX.len();
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
            for (sensor_idx, sensor) in states.iter_mut().enumerate() {
                for (axis_idx, state) in sensor.iter_mut().enumerate() {
                    *state = AlarmState::from_byte(payload[sensor_idx * 3 + axis_idx]);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_frame_received_in_chunks() {
        let frame = [0xAA, 0x55, 0x81, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0x55, 0xAA];
        let mut parser = FrameParser::default();

        assert!(parser.push(&frame[..5]).is_empty());
        let parsed = parser.push(&frame[5..]);

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].kind, ParsedFrameKind::Status);
        assert_eq!(parsed[0].states[0][1], AlarmState::Alarm);
        assert_eq!(parsed[0].raw, frame);
    }

    #[test]
    fn skips_noise_before_a_valid_frame() {
        let bytes = [
            0x10, 0x20, 0xAA, 0x55, 0x83, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x55, 0xAA,
        ];
        let parsed = FrameParser::default().push(&bytes);

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].kind, ParsedFrameKind::SelfTestResult);
    }
}
