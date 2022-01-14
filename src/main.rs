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
    sequence: VecDeque<u32>,
    ack: VecDeque<u32>,
    transaction: VecDeque<u16>,
    protocol: VecDeque<u16>,
    len: VecDeque<u16>,
    unit_id: VecDeque<u8>,
    function: VecDeque<u8>,
    ref_number: VecDeque<u16>,
    data: VecDeque<u16>,
    mult_count: VecDeque<u8>,
    mult_data: VecDeque<String>,
    duration: VecDeque<i64>
}

struct RequestSaves {
    times: DateTime<Utc>,
    packet: PacketAttr,
    duration: i64
}

struct CheckData {
    mac: Vec<String>,
    addr: Vec<String>,
    port: Vec<u16>,
    protocol: Vec<u16>,
    len: Vec<u16>,
    unit_id: Vec<u8>,
    function: Vec<u8>,
    ref_number: Vec<u16>,
    data: Vec<u16>,
    mult_count: Vec<u8>,
    mult_data: Vec<String>,
    dur: Vec<i64>
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
            sequence: VecDeque::<u32>::new(),
            ack: VecDeque::<u32>::new(),
            transaction: VecDeque::<u16>::new(),
            protocol: VecDeque::<u16>::new(),
            len: VecDeque::<u16>::new(),
            unit_id: VecDeque::<u8>::new(),
            function: VecDeque::<u8>::new(),
            ref_number: VecDeque::<u16>::new(),
            data: VecDeque::<u16>::new(),
            mult_count: VecDeque::<u8>::new(),
            mult_data: VecDeque::<String>::new(),
            duration: VecDeque::<i64>::new(),
        }
    }

    fn push_back(&mut self, pa: &RequestSaves) {
        let ndt: NaiveDateTime = pa.times.naive_local();
        self.datetime.push_back(ndt.timestamp());
        self.datetime_subsec.push_back(ndt.timestamp_subsec_nanos());
        self.src_mac.push_back(pa.packet.src_mac.to_string());
        self.dst_mac.push_back(pa.packet.dst_mac.to_string());
        self.src_addr.push_back(pa.packet.src_addr.to_string());
        self.dst_addr.push_back(pa.packet.dst_addr.to_string());
        self.src_port.push_back(pa.packet.src_port);
        self.dst_port.push_back(pa.packet.dst_port);
        self.length.push_back(pa.packet.length);
        self.sequence.push_back(pa.packet.sequence);
        self.ack.push_back(pa.packet.ack);
        self.transaction.push_back(pa.packet.transaction);
        self.protocol.push_back(pa.packet.protocol);
        self.len.push_back(pa.packet.len);
        self.unit_id.push_back(pa.packet.unit_id);
        self.function.push_back(pa.packet.function);
        self.ref_number.push_back(pa.packet.ref_number);
        self.data.push_back(pa.packet.data);
        self.mult_count.push_back(pa.packet.mult_count);
        self.mult_data.push_back(pa.packet.mult_data.clone());
        self.duration.push_back(pa.duration);
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
        let sequence = self.sequence.pop_front().unwrap();
        let ack = self.ack.pop_front().unwrap();
        let transaction = self.transaction.pop_front().unwrap();
        let protocol = self.protocol.pop_front().unwrap();
        let len = self.len.pop_front().unwrap();
        let unit_id = self.unit_id.pop_front().unwrap();
        let function = self.function.pop_front().unwrap();
        let ref_number = self.ref_number.pop_front().unwrap();
        let data = self.data.pop_front().unwrap();
        let mult_count = self.mult_count.pop_front().unwrap();
        let mult_data = self.mult_data.pop_front().unwrap();
        let duration = self.duration.pop_front().unwrap();
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
        self.sequence.clear();
        self.ack.clear();
        self.transaction.clear();
        self.protocol.clear();
        self.len.clear();
        self.unit_id.clear();
        self.function.clear();
        self.ref_number.clear();
        self.data.clear();
        self.mult_count.clear();
        self.mult_data.clear();
        self.duration.clear();
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
                        Field::new("Sequence", DataType::UInt32, false),        // 9
                        Field::new("Ack", DataType::UInt32, false),             // 10
                        Field::new("Transaction", DataType::UInt16, false),     // 11
                        Field::new("Protocol", DataType::UInt16, false),        // 12
                        Field::new("Len", DataType::UInt16, false),             // 13
                        Field::new("UnitID", DataType::UInt8, false),           // 14
                        Field::new("Function", DataType::UInt8, false),         // 15
                        Field::new("ReferenceNumber", DataType::UInt16, false), // 16
                        Field::new("Data", DataType::UInt16, false),            // 17
                        Field::new("MultCount", DataType::UInt8, false),        // 18
                        Field::new("MultData", DataType::Utf8, false),          // 19
                        Field::new("Duration", DataType::Int64, false),         // 20
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
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt32Type>::from_iter_values(self.sequence.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt32Type>::from_iter_values(self.ack.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.transaction.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.protocol.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.len.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(self.unit_id.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(self.function.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.ref_number.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.data.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(self.mult_count.range(win_front..win_back).cloned())),
            Arc::new(StringArray::from_iter_values(self.mult_data.range(win_front..win_back).cloned())),
            Arc::new(PrimitiveArray::<arrow::datatypes::Int64Type>::from_iter_values(self.duration.range(win_front..win_back).cloned())),
            ])?;
        Ok(batch)
    }
}

impl RequestSaves {
    fn new(utc: &DateTime<Utc>, re_packet: PacketAttr) -> Self{
        Self {
            times: utc.clone(),
            packet: re_packet,
            duration: 0,
        }
    }

    fn pair_check(&mut self, ch_packet: &PacketAttr) -> bool{
        if self.packet.src_mac != ch_packet.dst_mac {
            return false;
        }
        if self.packet.dst_mac != ch_packet.src_mac {
            return false;
        }
        if self.packet.src_addr != ch_packet.dst_addr {
            return false;
        }
        if self.packet.dst_addr != ch_packet.src_addr {
            return false;
        }
        if self.packet.src_port != ch_packet.dst_port {
            return false;
        }
        if self.packet.dst_port != ch_packet.src_port {
            return false;
        }
        if self.packet.transaction != ch_packet.transaction {
            return false;
        }
        if self.packet.protocol != ch_packet.protocol {
            return false;
        }
        if self.packet.function != ch_packet.function {
            return false;
        }
        self.packet = ch_packet.clone();
        return true;
    }

    fn difference(&mut self, utc: &DateTime<Utc>){
        let after_time = utc.clone();
        let duration: chrono::Duration = after_time - self.times;
        match duration.num_microseconds() {
            Some(t) => self.duration = t,
            None => self.duration = -1,
        }
        self.times = after_time;
    }
}

impl CheckData {
    fn new() -> Self {
        Self {
            mac: Vec::new(),
            addr: Vec::new(),
            port: Vec::new(),
            protocol: Vec::new(),
            len: Vec::new(),
            unit_id: Vec::new(),
            function: Vec::new(),
            ref_number: Vec::new(),
            data: Vec::new(),
            mult_count: Vec::new(),
            mult_data: Vec::new(),
            dur: Vec::new(),
        }
    }

    fn def_append(&mut self, pa: &PacketAttr) {
        if self.mac.iter().any(|e| e==&pa.src_mac.to_string()) == false {
            self.mac.push(pa.src_mac.to_string());
        }
        if self.mac.iter().any(|e| e==&pa.dst_mac.to_string()) == false {
            self.mac.push(pa.dst_mac.to_string());
        }
        if self.addr.iter().any(|e| e==&pa.src_addr.to_string()) == false {
            self.addr.push(pa.src_addr.to_string());
        }
        if self.addr.iter().any(|e| e==&pa.dst_addr.to_string()) == false {
            self.addr.push(pa.dst_addr.to_string());
        }
        if self.port.iter().any(|e| e==&pa.src_port) == false {
            self.port.push(pa.src_port);
        }
        if self.port.iter().any(|e| e==&pa.dst_port) == false {
            self.port.push(pa.dst_port);
        }
        if self.protocol.iter().any(|e| e==&pa.protocol) == false {
            self.protocol.push(pa.protocol);
        }
        if self.len.iter().any(|e| e==&pa.len) == false {
            self.len.push(pa.len);
        }
        if self.unit_id.iter().any(|e| e==&pa.unit_id) == false {
            self.unit_id.push(pa.unit_id);
        }
        if self.function.iter().any(|e| e==&pa.function) == false {
            self.function.push(pa.function);
        }
        if self.ref_number.iter().any(|e| e==&pa.ref_number) == false {
            self.ref_number.push(pa.ref_number);
        }
        if self.data.iter().any(|e| e==&pa.data) == false {
            self.data.push(pa.data);
        }
        if self.mult_count.iter().any(|e| e==&pa.mult_count) == false {
            self.mult_count.push(pa.mult_count);
        }
        if self.mult_data.iter().any(|e| e==&pa.mult_data) == false {
            self.mult_data.push(pa.mult_data.clone());
        }
    }

    fn duration_append(&mut self, du: i64) {
        println!("duration {}", du);
        if du > 0 {
            self.dur.push(du);
            self.dur.sort();
        }
    }

    fn get_schema(&self) -> Arc<Schema> {
        let schema = Arc::new(
            Schema::new(vec![
                    Field::new("mac", DataType::Utf8, false),
                    Field::new("addr", DataType::Utf8, false),
                    Field::new("port", DataType::UInt16, false),
                    Field::new("protocol", DataType::UInt16, false),
                    Field::new("len", DataType::UInt16, false),
                    Field::new("unit_id", DataType::UInt8, false),
                    Field::new("function", DataType::UInt8, false),
                    Field::new("ref_number", DataType::UInt16, false),
                    Field::new("data", DataType::UInt16, false),
                    Field::new("mult_count", DataType::UInt8, false),
                    Field::new("mult_data", DataType::Utf8, false),
                    Field::new("dur", DataType::Int64, false),
            ]));
        schema
    }

    fn same_size(&mut self){
        let mut max = 1;
        if self.mac.len() > max {
            max = self.mac.len();
        }
        if self.addr.len() > max {
            max = self.addr.len();
        }
        if self.port.len() > max {
            max = self.port.len();
        }
        if self.protocol.len() > max {
            max = self.protocol.len();
        }
        if self.len.len() > max {
            max = self.len.len();
        }
        if self.unit_id.len() > max {
            max = self.unit_id.len();
        }
        if self.function.len() > max {
            max = self.function.len();
        }
        if self.ref_number.len() > max {
            max = self.ref_number.len();
        }
        if self.data.len() > max {
            max = self.data.len();
        }
        if self.mult_count.len() > max {
            max = self.mult_count.len();
        }
        if self.mult_data.len() > max {
            max = self.mult_data.len();
        }
        for i in self.mac.len()..max {
            self.mac.push("".to_string());
        }
        for i in self.addr.len()..max {
            self.addr.push("".to_string());
        }
        for i in self.port.len()..max {
            self.port.push(0);
        }
        for i in self.protocol.len()..max {
            self.protocol.push(0);
        }
        for i in self.len.len()..max {
            self.len.push(0);
        }
        for i in self.unit_id.len()..max {
            self.unit_id.push(0);
        }
        for i in self.function.len()..max {
            self.function.push(0);
        }
        for i in self.ref_number.len()..max {
            self.ref_number.push(0);
        }
        for i in self.data.len()..max {
            self.data.push(0);
        }
        for i in self.mult_count.len()..max {
            self.mult_count.push(0);
        }
        for i in self.mult_data.len()..max {
            self.mult_data.push("".to_string());
        }
        if self.dur.len() > 0 {
            let quartile1 = (self.dur[(self.dur.len() / 4)] as f64);
            let quartile3 = (self.dur[(self.dur.len() / 4 * 3)] as f64);
            let quartile = quartile3 - quartile1;
            let check = (( quartile3 + quartile ) * 1.5 ) as i64;
        // 12月4日の進捗
            self.dur.clear();
            self.dur.push(check);
        }
        else {
            self.dur.clear();
            self.dur.push(-1);
        }
        for i in self.dur.len()..max {
            self.dur.push(0);
        }
    }

    fn get_batch(&self, schema: &Schema) -> datafusion::error::Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
            Arc::new(StringArray::from_iter_values(self.mac.clone())),
            Arc::new(StringArray::from_iter_values(self.addr.clone())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.port.clone())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.protocol.clone())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.len.clone())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(self.unit_id.clone())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(self.function.clone())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.ref_number.clone())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(self.data.clone())),
            Arc::new(PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(self.mult_count.clone())),
            Arc::new(StringArray::from_iter_values(self.mult_data.clone())),
            Arc::new(PrimitiveArray::<arrow::datatypes::Int64Type>::from_iter_values(self.dur.clone())),
            ])?;
        Ok(batch)
    }

    fn printer(&self){
        println!("mac {:?}", self.mac);
        println!("addr {:?}", self.addr);
        println!("port {:?}", self.port);
        println!("protocol {:?}", self.protocol);
        println!("len {:?}", self.len);
        println!("unit_id {:?}", self.unit_id);
        println!("function {:?}", self.function);
        println!("ref_number {:?}", self.ref_number);
        println!("data {:?}", self.data);
        println!("mult_count {:?}", self.mult_count);
        println!("mult_data {:?}", self.mult_data);
        println!("check {:?}", self.dur);
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

async fn do_put_check_data(client: &mut FlightServiceClient<tonic::transport::channel::Channel>, check_data: &mut CheckData) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let schema = check_data.get_schema();
    let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
    let mut flight_data_schema: FlightData = SchemaAsIpc::new(schema.as_ref(), &options).into();
    flight_data_schema.flight_descriptor = Some(FlightDescriptor {
        r#type: flight_descriptor::DescriptorType::Path as i32,
        cmd: vec![],
        path: vec![format!("check_data")],
    });
    log::info!("{:?}", flight_data_schema);
    let flight_data_batch = flight_data_from_arrow_batch(&check_data.get_batch(&schema).unwrap(), &options);
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
    // confirm available interfaces
    //for _iface in interfaces.iter() {
    //    println!("{}", _iface.name);
    //}
    
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


    //#[cfg(target_os = "linux")]
    let channel = datalink::channel;
    
    //#[cfg(target_os = "macos")]
    //let channel = datalink::pcap::channel;

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
    /*
    #[cfg(target_os = "linux")]
    pthread_set_cpu(handle2.as_pthread_t(), 1usize);
    */
    barrier.wait();
    // ** set affinity using libc **
    // record packet attributes as vectors
    // struct PacketAttr {
    //     interface_name: String,
    //     src_addr: IpAddr,
    //     dst_addr: IpAddr,
    //     src_port: u16,
    //     dst_port: u16,
    //     length: usize
    // }
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
    //let mut client: FlightServiceClient<tonic::transport::channel::Channel> = FlightServiceClient::connect("http://172.16.0.2:5005").await?;
    let mut client = FlightServiceClient::connect("http://localhost:5005").await?;
    // Create packet buffer
    //let mut if_packets: HashMap<String, IfPackets> = HashMap::new();
    let mut if_packets: IfPackets = IfPackets::new();
    let mut keep_data: Vec<RequestSaves> = Vec::new();
    let mut check_data: CheckData = CheckData::new();
    /*
    if per_iface {
        for iface in ifaces_name.clone() {
            if_packets.insert(iface, IfPackets::new());
        }
    } else {
        if_packets.insert("all".to_string(), IfPackets::new());
    }
    */
    let mut first_flag = false;
    let mut counter = 0;
    loop {
        tokio::select! {
            Some(v) = log_receiver.recv() => {
                let utc: DateTime<Utc> = Utc::now();
                if first_flag != true {
                    first_flag = true;
                }
                //let keep = BackUps::new(&utc, v.src_mac.to_string(), v.dst_mac.to_string(), v.src_addr.to_string(), v.dst_addr.to_string(), v.src_port, v.dst_port, v.transaction, v.protocol, v.function);
                if counter < 60 {
                    check_data.def_append(&v);
                }
                match (v.src_port, v.dst_port) {
                    (_, 502) => { //Request
                        let keep = RequestSaves::new(&utc, v);
                        if_packets.push_back(&keep);
                        if keep_data.len() >= MAX_SUB_LEN {
                            keep_data.remove(0);
                        }
                        keep_data.push(keep);
                    }
                    (502, _) => { //Reply
                        let mut check_flag = true;
                        for i in 0..keep_data.len() {
                            if keep_data[i].pair_check(&v) {
                                keep_data[i].difference(&utc);
                                check_flag = false;
                                if counter < 60 {
                                    check_data.duration_append(keep_data[i].duration.clone());
                                }
                                if_packets.push_back(&keep_data[i]);
                                keep_data.remove(i);
                                break;
                            }
                        }
                        if check_flag {
                            let keep = RequestSaves::new(&utc, v);
                            if_packets.push_back(&keep);
                        }
                    }
                    (_, _) => {}
                }
                if win_back < MAX_LEN { win_back += 1 }
                // update time-based window index
                // win_back is used as win_front..win_back in get_batch
                // it means win_front <= X < win_back
                log::info!("add a packet");
                match window_type {
                    "row" => {
                        // row-based
                        //if if_packets[&if_name].len() > MAX_LEN {
                        if if_packets.len() > MAX_LEN {
                            do_put_flight_data(&mut client, &mut if_packets, &utc, &win_fronts[k], &win_back).await?;

                            log::info!("do_put IfPackets (row-based)");
                            //if_packets.get_mut(&if_name).unwrap().clear();
                            if_packets.clear();
                            // reset time-based window index (currently not used in row-based mode)
                            win_back = 0;
                        }
                    },
                    "time" => {
                        // time-based
                        // remove the oldest packet
                        //if if_packets[&if_name].len() > MAX_LEN {
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
                let utc: DateTime<Utc> = Utc::now();
                if first_flag {
                    if counter <= 60 {
                        counter += 1;
                    }
                }

                println!("{}, {}", win_fronts[k], win_back);
                if win_fronts[k] != win_back {
                    match window_type {
                        "row" => {
                        },
                        "time" => {
                            // increment counter
                            //k = (k + 1) % n;
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
                if counter == 60 {
                    check_data.printer();
                    check_data.same_size();
                    do_put_check_data(&mut client, &mut check_data).await?;
                }
            },
        }
    }
    handle1.join().unwrap();
}
