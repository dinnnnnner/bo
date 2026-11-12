use serialport::{SerialPort, SerialPortType};
use std::{
    collections::{HashMap, HashSet},
    io::{self, Read, Write},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, Sender},
    },
    thread,
    time::Duration,
};

pub const BAUD_RATE: u32 = 115_200;
const SCAN_INTERVAL: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
enum PortCommand {
    Write(Vec<u8>),
    Shutdown,
}

#[derive(Debug)]
pub enum PortEvent {
    Connected(String),
    Data { port: String, data: Vec<u8> },
    Disconnected { port: String, reason: String },
}

#[derive(Clone)]
pub struct SerialManager {
    ports: Arc<Mutex<HashMap<String, Sender<PortCommand>>>>,
}

impl SerialManager {
    pub fn new() -> Self {
        Self {
            ports: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn start(&self, event_tx: Sender<PortEvent>) {
        let ports = Arc::clone(&self.ports);

        thread::spawn(move || {
            loop {
                match serialport::available_ports() {
                    Ok(available_ports) => {
                        let current: HashSet<String> = available_ports
                            .iter()
                            .map(|port| port.port_name.clone())
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

    pub fn send_to(&self, port_name: &str, data: impl Into<Vec<u8>>) -> io::Result<()> {
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

    pub fn send_all(&self, data: impl Into<Vec<u8>>) {
        let data = data.into();
        let ports = self.ports.lock().expect("ports lock poisoned");

        for (port_name, command_tx) in ports.iter() {
            if command_tx.send(PortCommand::Write(data.clone())).is_err() {
                eprintln!("send to {port_name} failed: serial worker stopped");
            }
        }
    }

    pub fn list_ports(&self) -> Vec<String> {
        let ports = self.ports.lock().expect("ports lock poisoned");
        let mut names = ports.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    #[cfg(test)]
    pub fn add_test_port(&self, name: &str) -> impl Fn() -> Vec<Vec<u8>> {
        let (tx, rx) = mpsc::channel();
        self.ports.lock().unwrap().insert(name.to_string(), tx);
        move || {
            rx.try_iter()
                .filter_map(|command| match command {
                    PortCommand::Write(data) => Some(data),
                    PortCommand::Shutdown => None,
                })
                .collect()
        }
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
        SerialPortType::UsbPort(usb) => println!(
            "found {} USB vid={:04x} pid={:04x}",
            info.port_name, usb.vid, usb.pid
        ),
        SerialPortType::BluetoothPort => println!("found {} Bluetooth", info.port_name),
        SerialPortType::PciPort => println!("found {} PCI", info.port_name),
        SerialPortType::Unknown => println!("found {}", info.port_name),
    }
}
