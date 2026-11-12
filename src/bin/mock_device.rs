use serialport::SerialPort;
use std::{
    env,
    io::{self, Read},
    thread,
    time::{Duration, Instant},
};

const DEFAULT_BAUD: u32 = 115_200;
const READ_TIMEOUT: Duration = Duration::from_millis(50);
const STATUS_FRAME_LEN: usize = 14;
const THRESHOLD_FRAME_LEN: usize = 17;
const STATUS_HEAD: [u8; 3] = [0xAA, 0x55, 0x81];
const THRESHOLD_HEAD: [u8; 3] = [0xAA, 0x55, 0x01];
const FRAME_TAIL: [u8; 2] = [0x55, 0xAA];

#[derive(Debug)]
struct Config {
    ports: Vec<String>,
    baud: u32,
}

fn main() {
    let Some(config) = parse_args() else {
        print_usage();
        return;
    };

    println!(
        "starting {} mock devices @ {} baud",
        config.ports.len(),
        config.baud
    );

    let mut handles = Vec::new();
    for (idx, port_name) in config.ports.into_iter().enumerate() {
        let baud = config.baud;
        handles.push(thread::spawn(move || {
            run_device(idx as u64, port_name, baud)
        }));
    }

    for handle in handles {
        if let Err(err) = handle.join() {
            eprintln!("mock device thread panicked: {err:?}");
        }
    }
}

fn parse_args() -> Option<Config> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        return None;
    }

    let mut ports = Vec::new();
    let mut baud = DEFAULT_BAUD;
    let mut idx = 0;

    while idx < args.len() {
        match args[idx].as_str() {
            "--ports" => {
                idx += 1;
                while idx < args.len() && !args[idx].starts_with("--") {
                    ports.push(args[idx].clone());
                    idx += 1;
                }
            }
            "--baud" => {
                idx += 1;
                baud = args.get(idx)?.parse::<u32>().ok()?;
                idx += 1;
            }
            port if !port.starts_with("--") => {
                ports.push(port.to_string());
                idx += 1;
            }
            _ => return None,
        }
    }

    if ports.is_empty() {
        None
    } else {
        Some(Config { ports, baud })
    }
}

fn print_usage() {
    eprintln!("usage:");
    eprintln!("  cargo run --bin mock_device -- --ports COM3 COM9 --baud 115200");
    eprintln!("  cargo run --bin mock_device -- COM3 COM9");
}

fn run_device(device_idx: u64, port_name: String, baud: u32) {
    let result = (|| -> serialport::Result<()> {
        let mut port = serialport::new(&port_name, baud)
            .timeout(READ_TIMEOUT)
            .open()?;

        println!("[{port_name}] mock device opened @ {baud}");
        let mut rx_buf = Vec::new();
        let mut chunk = [0_u8; 256];
        let mut frame_index = 0_u64;
        let mut next_send = Instant::now() + Duration::from_millis(device_idx * 120);

        loop {
            match port.read(&mut chunk) {
                Ok(n) if n > 0 => {
                    rx_buf.extend_from_slice(&chunk[..n]);
                    for thresholds in drain_threshold_frames(&mut rx_buf) {
                        println!(
                            "[{port_name}] threshold received: x_hi={} x_lo={} y_hi={} y_lo={} z_hi={} z_lo={}",
                            thresholds[0],
                            thresholds[1],
                            thresholds[2],
                            thresholds[3],
                            thresholds[4],
                            thresholds[5]
                        );
                    }
                }
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                Err(err) => return Err(err.into()),
            }

            if Instant::now() >= next_send {
                let frame = build_status_frame(frame_index, device_idx);
                write_all_with_retry(&mut *port, &frame)?;
                println!(
                    "[{port_name}] status sent #{frame_index}: {}",
                    hex_string(&frame)
                );
                frame_index = frame_index.wrapping_add(1);
                next_send += Duration::from_millis(500);
            }

            thread::sleep(Duration::from_millis(5));
        }
    })();

    if let Err(err) = result {
        eprintln!("[{port_name}] stopped: {err}");
    }
}

fn build_status_frame(frame_index: u64, device_idx: u64) -> [u8; STATUS_FRAME_LEN] {
    let mut frame = [0_u8; STATUS_FRAME_LEN];
    frame[..3].copy_from_slice(&STATUS_HEAD);
    frame[12..].copy_from_slice(&FRAME_TAIL);

    for idx in 0..9 {
        frame[3 + idx] = if (frame_index + idx as u64 + device_idx * 2).is_multiple_of(7) {
            0x01
        } else {
            0x00
        };
    }

    frame
}

fn drain_threshold_frames(buffer: &mut Vec<u8>) -> Vec<[i16; 6]> {
    let mut frames = Vec::new();

    loop {
        let Some(pos) = find_head(buffer, &THRESHOLD_HEAD) else {
            keep_possible_head_suffix(buffer);
            break;
        };
        if pos > 0 {
            buffer.drain(..pos);
        }
        if buffer.len() < THRESHOLD_FRAME_LEN {
            break;
        }
        if buffer[THRESHOLD_FRAME_LEN - 2..THRESHOLD_FRAME_LEN] != FRAME_TAIL {
            buffer.drain(..1);
            continue;
        }

        let mut values = [0_i16; 6];
        for (idx, value) in values.iter_mut().enumerate() {
            let offset = 3 + idx * 2;
            *value = i16::from_le_bytes([buffer[offset], buffer[offset + 1]]);
        }
        frames.push(values);
        buffer.drain(..THRESHOLD_FRAME_LEN);
    }

    frames
}

fn find_head(buffer: &[u8], head: &[u8; 3]) -> Option<usize> {
    buffer.windows(head.len()).position(|window| window == head)
}

fn keep_possible_head_suffix(buffer: &mut Vec<u8>) {
    if buffer.len() > 2 {
        let drop_len = buffer.len() - 2;
        buffer.drain(..drop_len);
    }
}

fn write_all_with_retry(port: &mut dyn SerialPort, data: &[u8]) -> serialport::Result<()> {
    let mut written = 0;
    while written < data.len() {
        match port.write(&data[written..]) {
            Ok(0) => thread::sleep(Duration::from_millis(5)),
            Ok(n) => written += n,
            Err(err) if err.kind() == io::ErrorKind::TimedOut => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(err) => return Err(err.into()),
        }
    }
    port.flush()?;
    Ok(())
}

fn hex_string(data: &[u8]) -> String {
    data.iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(" ")
}
