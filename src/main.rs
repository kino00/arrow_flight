#[cfg(target_os = "linux")]
use libc;

use log;
mod packet_handler;
use packet_handler::{PacketAttr, Action};

#[cfg(target_os = "linux")]
use std::os::unix::thread::JoinHandleExt;

use std::env;
use std::io::{self, Write};
//use std::net::IpAddr;
//use std::net::{AddrParseError, IpAddr, Ipv4Addr};
use std::process;
use std::thread;
use std::sync::{Arc, Barrier};
use tokio::sync::mpsc;

use pnet;
use pnet::datalink;
use pnet::datalink::{Channel, NetworkInterface, /*DataLinkSender,*/ DataLinkReceiver};
//use pnet::datalink::{Channel, MacAddr, NetworkInterface};

use pnet::packet::ethernet::{/*EtherTypes,*/ EthernetPacket};
//use pnet::packet::Packet;

//use std::iter::FromIterator;
use std::collections::{HashMap, VecDeque};
use chrono::{Utc, DateTime, NaiveDateTime};
//use num_traits::cast::ToPrimitive;

use tokio::time::{/*sleep,*/ interval_at, /*Duration,*/ Instant};
//use tonic::IntoStreamingRequest;
use futures_util::stream;
//use futures_core::stream::Stream;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;

use arrow_flight::{
    FlightDescriptor, /*Ticket,*/ FlightData, SchemaAsIpc,
    flight_descriptor,
    flight_service_client::FlightServiceClient,
    utils::{flight_data_from_arrow_batch/*, flight_data_to_arrow_batch*/},
};

use datafusion::arrow::array::{/*Int64Array, UInt32Array, UInt16Array,*/ StringArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
//use datafusion::arrow::util::pretty;


// maximum buffer length (TODO: should be a parameter)
//pub const MAX_LEN: usize = 10;
//pub const MAX_LEN: usize = 1024;
//pub const MAX_LEN: usize = 4096;
pub const MAX_LEN: usize = 65536;
pub const MAX_SUB_LEN: usize = 64;

// Packet record buffer for each interface
struct IfPackets {
    datetime: VecDeque<i64>,
    datetime_subsec: VecDeque<u32>,
    src_mac: VecDeque<String>,
    dst_mac: VecDeque<String>,
    src_addr: VecDeque<String>,
    dst_addr: VecDeque<String>,
    src_port: VecDeque<u16>,
    dst_port: VecDeque<u16>,
    length: VecDeque<u32>,
    transaction: VecDeque<u16>,
    protocol: VecDeque<u16>,
    len: VecDeque<u16>,
    unit_id: VecDeque<u8>,
    function: VecDeque<u8>,
    ref_number: VecDeque<u16>,
    data: VecDeque<u16>,
    mult_count: VecDeque<u8>,
    mult_data: VecDeque<String>
}

impl IfPackets {
    pub fn new() -> Self {
        Self {
            datetime: VecDeque::<i64>::new(),
            datetime_subsec: VecDeque::<u32>::new(),
            src_mac: VecDeque::<String>::new(),
            dst_mac: VecDeque::<String>::new(),
            src_addr: VecDeque::<String>::new(),
            dst_addr: VecDeque::<String>::new(),
            src_port: VecDeque::<u16>::new(),
            dst_port: VecDeque::<u16>::new(),
            length: VecDeque::<u32>::new(),
            transaction: VecDeque::<u16>::new(),
            protocol: VecDeque::<u16>::new(),
            len: VecDeque::<u16>::new(),
            unit_id: VecDeque::<u8>::new(),
            function: VecDeque::<u8>::new(),
            ref_number: VecDeque::<u16>::new(),
            data: VecDeque::<u16>::new(),
            mult_count: VecDeque::<u8>::new(),
            mult_data: VecDeque::<String>::new(),
        }
    }

    fn push_back(&mut self, pa: PacketAttr, utc: &DateTime<Utc>) {
        let ndt: NaiveDateTime = utc.naive_local();
        self.datetime.push_back(ndt.timestamp());
        self.datetime_subsec.push_back(ndt.timestamp_subsec_nanos());
        self.src_mac.push_back(pa.src_mac.to_string());
        self.dst_mac.push_back(pa.dst_mac.to_string());
        self.src_addr.push_back(pa.src_addr.to_string());
        self.dst_addr.push_back(pa.dst_addr.to_string());
        self.src_port.push_back(pa.src_port);
        self.dst_port.push_back(pa.dst_port);
        self.length.push_back(pa.length);
        self.transaction.push_back(pa.protocol);
        self.protocol.push_back(pa.protocol);
        self.len.push_back(pa.len);
        self.unit_id.push_back(pa.unit_id);
        self.function.push_back(pa.function);
        self.ref_number.push_back(pa.ref_number);
        self.data.push_back(pa.data);
        self.mult_count.push_back(pa.mult_count);
        self.mult_data.push_back(pa.mult_data.clone());
    }

    fn pop_front(&mut self) {
        let datetime = self.datetime.pop_front().unwrap();
        let datetime_subsec = self.datetime_subsec.pop_front().unwrap();
        let src_mac = self.src_mac.pop_front().unwrap();
        let dst_mac = self.dst_mac.pop_front().unwrap();
        let src_addr = self.src_addr.pop_front().unwrap();
        let dst_addr = self.dst_addr.pop_front().unwrap();
        let src_port = self.src_port.pop_front().unwrap();
        let dst_port = self.dst_port.pop_front().unwrap();
        let length = self.length.pop_front().unwrap();
        let transaction = self.transaction.pop_front().unwrap();
        let protocol = self.protocol.pop_front().unwrap();
        let len = self.len.pop_front().unwrap();
        let unit_id = self.unit_id.pop_front().unwrap();
        let function = self.function.pop_front().unwrap();
        let ref_number = self.ref_number.pop_front().unwrap();
        let data = self.data.pop_front().unwrap();
        let mult_count = self.mult_count.pop_front().unwrap();
        let mult_data = self.mult_data.pop_front().unwrap();
    }

    fn clear(&mut self) {
        self.datetime.clear();
        self.datetime_subsec.clear();
        self.src_mac.clear();
        self.dst_mac.clear();
        self.src_addr.clear();
        self.dst_addr.clear();
        self.src_port.clear();
        self.dst_port.clear();
        self.length.clear();
        self.transaction.clear();
        self.protocol.clear();
        self.len.clear();
        self.unit_id.clear();
        self.function.clear();
        self.ref_number.clear();
        self.data.clear();
        self.mult_count.clear();
        self.mult_data.clear();
    }

    fn len(&self) -> usize {
        self.datetime.len()
    }

    fn get_schema(&self) -> Arc<Schema> {
        let schema = Arc::new(
            Schema::new(vec![
                        Field::new("DateTime", DataType::Int64, false),         // 0
                        Field::new("DateTimeSubsec", DataType::UInt32, false),  // 1
                        Field::new("SrcMAC", DataType::Utf8, false),            // 2
                        Field::new("DstMAC", DataType::Utf8, false),            // 3
                        Field::new("SrcIP", DataType::Utf8, false),             // 4
                        Field::new("DstIP", DataType::Utf8, false),             // 5
                        Field::new("SrcPort", DataType::UInt16, false),         // 6
                        Field::new("DstPort", DataType::UInt16, false),         // 7
                        Field::new("TCPLen", DataType::UInt32, false),          // 8
                        Field::new("Transaction", DataType::UInt16, false),     // 9
                        Field::new("Protocol", DataType::UInt16, false),        // 10
                        Field::new("Len", DataType::UInt16, false),             // 11
                        Field::new("UnitID", DataType::UInt8, false),           // 12
                        Field::new("Function", DataType::UInt8, false),         // 13
                        Field::new("ReferenceNumber", DataType::UInt16, false), // 14
                        Field::new("Data", DataType::UInt16, false),            // 15
                        Field::new("MultCount", DataType::UInt8, false),        // 16
                        Field::new("MultData", DataType::Utf8, false),          // 17
            ]));
        schema
    }

    fn get_batch(&self, schema: &Schema, win_front: &usize, win_back: &usize) -> datafusion::error::Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
            Arc::new(PrimitiveArray::<arrow::datatypes::Int64Type>::from_iter_values(self.datetime.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt32Type>::from_iter_values(self.datetime_subsec.range(win_front..win_back).cloned())),
            Arc::new(StringArray::from_iter_values(self.src_mac.range(win_front..win_back).cloned())),
            Arc::new(StringArray::from_iter_values(self.dst_mac.range(win_front..win_back).cloned())),
            Arc::new(StringArray::from_iter_values(self.src_addr.range(win_front..win_back).cloned())),
            Arc::new(StringArray::from_iter_values(self.dst_addr.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.src_port.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.dst_port.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt32Type>::from_iter_values(self.length.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.transaction.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.protocol.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.len.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(self.unit_id.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(self.function.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.ref_number.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.data.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(self.mult_count.range(win_front..win_back).cloned())),
            Arc::new(StringArray::from_iter_values(self.mult_data.range(win_front..win_back).cloned())),
            ])?;
        Ok(batch)
    }
}

fn packet_forwarding_thread(
    name: &str,
    iface: NetworkInterface,
    mut receiver: Box<dyn DataLinkReceiver>,
    mut log_sender: mpsc::Sender<PacketAttr>,
    b: Arc<Barrier>
) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
    .name(name.to_string())
    .spawn(move || {
        b.wait();
        let handle = thread::current();
        let thread_name = handle.name().unwrap();
        #[cfg(target_os = "linux")]
        log::debug!("Thread {} on CPU {}", thread_name, unsafe { libc::sched_getcpu() });

        #[cfg(target_os = "macos")]
        log::debug!("Thread {} starts", thread_name);

        loop {
            match receiver.next() {
                Ok(packet) => {
                    log::debug!("Ethernet@{:?}", thread_name);
                    log::debug!("packet bytes ---");
                    log::debug!("{:x?}", packet);
                    log::debug!("---");
                    log::debug!("len: {} @{:?}", packet.len(), thread_name);
                    match packet_handler::handle_ethernet_frame(&iface, &EthernetPacket::new(packet).unwrap())
                    {
                        Some(Action::Log(packet_attr)) => {
                            match log_sender.try_send(packet_attr) {
                                Ok(_) => log::debug!(
                                    "log_sender: send packet_attr successfully: @{:?}",
                                    thread_name
                                    ),
                                Err(e) => log::debug!(
                                    "log_sender: send packet_attr error: {} @{:?}",
                                    e,
                                    thread_name
                                    ),
                            }
                        },
                        Some(Action::Drop(message)) => log::warn!(
                            "receive_loop: drop packet: {:?} @{:?}",
                            message,
                            thread_name
                        ),
                        _ => {}
                    }
                }
                Err(e) => log::error!(
                    "receive_loop: unable to receive packet: {} @{:?}",
                    e,
                    thread_name
                ),
            }
        }
    })
}

#[cfg(target_os = "linux")]
fn pthread_set_cpu(pthread: libc::pthread_t, cpu: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::pthread_setaffinity_np(pthread, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

async fn do_put_flight_data(client: &mut FlightServiceClient<tonic::transport::channel::Channel>, if_packets: &mut IfPackets, utc: &DateTime<Utc>, win_front: &usize, win_back: &usize) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let schema = if_packets.get_schema();
    let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
    let mut flight_data_schema: FlightData = SchemaAsIpc::new(schema.as_ref(), &options).into();
    flight_data_schema.flight_descriptor = Some(FlightDescriptor {
        r#type: flight_descriptor::DescriptorType::Path as i32,
        cmd: vec![],
        path: vec![format!("{}", utc.format("%Y-%m-%d-%H_%M_%S_%6f"))],
    });
    log::info!("{:?}", flight_data_schema);
    let flight_data_batch = flight_data_from_arrow_batch(&if_packets.get_batch(&schema, &win_front, &win_back).unwrap(), &options);
    log::info!("{:?}", flight_data_batch);
    client.do_put(stream::iter(vec![flight_data_schema, flight_data_batch.1])).await?;
    Ok(())
}

#[tokio::main]
pub async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = match env::args().len() {
        //4 => vec![env::args().nth(1).unwrap(), env::args().nth(2).unwrap(), env::args().nth(3).unwrap()],
        2 => vec![env::args().nth(1).unwrap()],
        _ => {
            writeln!(
                io::stderr(),
                "USAGE: otp_agent <NETWORK INTERFACE1>"
                //"USAGE: otp_agent <NETWORK INTERFACE1> <NETWORK INTERFACE2> <c(count)/t(timer)>"
            )
            .unwrap();
            process::exit(1);
        }
    };

    // should be specified as a parameter
    let window_type = "time";
    //let window_type = "row";
    // window_duration = output_interval * n
    // for row-based n = 1
    let n = if window_type == "time" { 5 } else { 1 };
    let output_interval = 1000;

    let interfaces = datalink::interfaces();
    
    let iface1 = interfaces
        .iter()
        .find(|iface| iface.name == args[0])
        .unwrap()
        .clone();
    let ifaces_name = [iface1.name.clone()];

    // crete a new datalink channel
    #[cfg(target_os = "linux")]
    let config: datalink::Config = Default::default();
    
    #[cfg(target_os = "macos")]
    let config: datalink::Config = Default::default();

    println!("{:?}", config);
    /*
    let config: datalink::Config = datalink::Config {
            write_buffer_size: 65535,
            read_buffer_size: 65535,
            //write_buffer_size: 4096,
            //read_buffer_size: 4096,
            read_timeout: None,
            write_timeout: None,
            channel_type: datalink::ChannelType::Layer2,
            bpf_fd_attempts: 1000,
            linux_fanout: None,
            promiscuous: true,
        };
        */

    let channel = datalink::channel;

    let (sender1, receiver1) = match channel(&iface1, config) {
        Ok(Channel::Ethernet(tx1, rx1)) => (tx1, rx1),
        Ok(_) => panic!("Unknown channel type"),
        Err(e) => panic!("Error happened {}", e),
    };

    let barrier = Arc::new(Barrier::new(2));

    let (mut log_sender, mut log_receiver): (mpsc::Sender<PacketAttr>, mpsc::Receiver<PacketAttr>) = mpsc::channel(1024);
    let handle1 = match packet_forwarding_thread("thread1", iface1, receiver1, /*sender2,*/ log_sender.clone(), barrier.clone()) {
        Ok(handle) => handle,
        Err(e) => panic!("Error creating thread1: {}", e),
    };
    // ** set affinity using libc **
    #[cfg(target_os = "linux")]
    pthread_set_cpu(handle1.as_pthread_t(), 0usize);
    barrier.wait();
    let start = Instant::now();
    let mut interval_stream = IntervalStream::new(
        if window_type == "time" {
            // sleep for output interval
            interval_at(start.clone(), tokio::time::Duration::from_millis(output_interval.clone()))
        } else {
            // sleep for 24 hours (daily execution)
            interval_at(start.clone(), tokio::time::Duration::from_secs(60 * 60 * 24))
        }
    );
    // sliding window parameters
    // output interval counter
    let mut k = 0;
    // time-based window index
    let mut win_fronts: Vec<usize> = vec![0; n];
    let mut win_back: usize = 0;

    // Create Flight client
    let mut client = FlightServiceClient::connect("http://localhost:5005").await?;
    // Create packet buffer
    let mut if_packets: IfPackets = IfPackets::new();
    
    loop {
        tokio::select! {
            Some(v) = log_receiver.recv() => {
                let utc: DateTime<Utc> = Utc::now();
                if_packets.push_back(v, &utc);

                if win_back < MAX_LEN { win_back += 1 }
                // update time-based window index
                // win_back is used as win_front..win_back in get_batch
                // it means win_front <= X < win_back
                log::info!("add a packet");
                match window_type {
                    "row" => {
                        // row-based
                        if if_packets.len() > MAX_LEN {
                            do_put_flight_data(&mut client, &mut if_packets, &utc, &win_fronts[k], &win_back).await?;

                            log::info!("do_put IfPackets (row-based)");
                            if_packets.clear();
                            // reset time-based window index (currently not used in row-based mode)
                            win_back = 0;
                        }
                    },
                    "time" => {
                        // time-based
                        // remove the oldest packet
                        if if_packets.len() > MAX_LEN {
                            log::info!("pop a packet (time-based)");
                            if_packets.pop_front();
                            for i in 0..win_fronts.len() {
                                if win_fronts[i] > 0 { win_fronts[i] -= 1 }
                            }
                        }
                    },
                    _ => {
                    }
                }
            },
            Some(v) = interval_stream.next() => {
                // TODO: 経過時間が window_duration 以下の場合は，
                // win_fronts[k] を初期化するだけ．window_durationを超えたら，
                // output_interval ごとに出力する．
                log::info!("{:?}", v);

                if win_fronts[k] != win_back {
                    match window_type {
                        "row" => {
                        },
                        "time" => {
                            // increment counter
                            do_put_flight_data(&mut client, &mut if_packets, &utc, &win_fronts[k], &win_back).await?;
                            log::info!("do_put IfPackets (time-based)");

                            k = (k + 1) % n;
                            // win_frontsの更新
                            win_fronts[k] = win_back;
                        },
                        _ => {
                        }
                    }
                }
            },
        }
    }
    handle1.join().unwrap();
}
