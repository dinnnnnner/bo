use serialport::SerialPort;
use std::{
    env,
    io::{self, Read},
    thread,
    time::Duration,
};

const DEFAULT_BAUD: u32 = 115_200;
const READ_TIMEOUT: Duration = Duration::from_millis(100);
const STATUS_FRAME_LEN: usize = 14;
const STATUS_HEAD: [u8; 3] = [0xAA, 0x55, 0x81];
const THRESHOLD_HEAD: [u8; 3] = [0xAA, 0x55, 0x01];
const FRAME_TAIL: [u8; 2] = [0x55, 0xAA];

#[derive(Debug)]
struct Config {
    ports: Vec<String>,
    baud: u32,
    thresholds: [i16; 6],
}

fn main() {
    let Some(config) = parse_args() else {
        print_usage();
        return;
    };

    println!(
        "starting host tester for {} ports @ {} baud",
        config.ports.len(),
        config.baud
    );

    let threshold_frame = build_threshold_frame(config.thresholds);
    println!("threshold frame: {}", hex_string(&threshold_frame));

    let mut handles = Vec::new();
    for port_name in config.ports {
        let frame = threshold_frame.clone();
        let baud = config.baud;
        handles.push(thread::spawn(move || run_host_port(port_name, baud, frame)));
    }

    for handle in handles {
        if let Err(err) = handle.join() {
            eprintln!("host tester thread panicked: {err:?}");
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
    let mut thresholds = [100, -100, 120, -120, 80, -80];
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
            "--thresholds" => {
                idx += 1;
                for value in &mut thresholds {
                    *value = args.get(idx)?.parse::<i16>().ok()?;
                    idx += 1;
                }
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
        Some(Config {
            ports,
            baud,
            thresholds,
        })
    }
}

fn print_usage() {
    eprintln!("usage:");
    eprintln!(
        "  cargo run --bin host_tester -- --ports COM4 COM10 --baud 115200 --thresholds 100 -100 120 -120 80 -80"
    );
    eprintln!("  cargo run --bin host_tester -- COM4 COM10");
}

fn run_host_port(port_name: String, baud: u32, threshold_frame: Vec<u8>) {
    let result = (|| -> serialport::Result<()> {
        let mut port = serialport::new(&port_name, baud)
            .timeout(READ_TIMEOUT)
            .open()?;

        println!("[{port_name}] host opened @ {baud}");
        write_all_with_retry(&mut *port, &threshold_frame)?;
        println!("[{port_name}] threshold sent");

        let mut rx_buf = Vec::new();
        let mut chunk = [0_u8; 256];

        loop {
            match port.read(&mut chunk) {
                Ok(n) if n > 0 => {
                    rx_buf.extend_from_slice(&chunk[..n]);
                    for states in drain_status_frames(&mut rx_buf) {
                        print_status(&port_name, states);
                    }
                }
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                Err(err) => return Err(err.into()),
            }
        }
    })();

    if let Err(err) = result {
        eprintln!("[{port_name}] stopped: {err}");
    }
}

fn build_threshold_frame(values: [i16; 6]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(17);
    frame.extend_from_slice(&THRESHOLD_HEAD);
    for value in values {
        frame.extend_from_slice(&value.to_le_bytes());
    }
    frame.extend_from_slice(&FRAME_TAIL);
    frame
}

fn drain_status_frames(buffer: &mut Vec<u8>) -> Vec<[[u8; 3]; 3]> {
    let mut frames = Vec::new();

    loop {
        let Some(pos) = find_head(buffer, &STATUS_HEAD) else {
            keep_possible_head_suffix(buffer);
            break;
        };
        if pos > 0 {
            buffer.drain(..pos);
        }
        if buffer.len() < STATUS_FRAME_LEN {
            break;
        }
        if buffer[STATUS_FRAME_LEN - 2..STATUS_FRAME_LEN] != FRAME_TAIL {
            buffer.drain(..1);
            continue;
        }

        let mut states = [[0_u8; 3]; 3];
        for sensor_idx in 0..3 {
            for axis_idx in 0..3 {
                states[sensor_idx][axis_idx] = buffer[3 + sensor_idx * 3 + axis_idx];
            }
        }
        frames.push(states);
        buffer.drain(..STATUS_FRAME_LEN);
    }

    frames
}

fn print_status(port_name: &str, states: [[u8; 3]; 3]) {
    println!(
        "[{port_name}] status: S1[x={} y={} z={}] S2[x={} y={} z={}] S3[x={} y={} z={}]",
        label(states[0][0]),
        label(states[0][1]),
        label(states[0][2]),
        label(states[1][0]),
        label(states[1][1]),
        label(states[1][2]),
        label(states[2][0]),
        label(states[2][1]),
        label(states[2][2])
    );
}

fn label(value: u8) -> &'static str {
    match value {
        0x00 => "normal",
        0x01 => "alarm",
        _ => "invalid",
    }
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
