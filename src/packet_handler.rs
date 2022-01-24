use log;
use std::net::IpAddr;
//use std::net::{AddrParseError, IpAddr, Ipv4Addr};

use pnet;
use pnet::datalink::{NetworkInterface, MacAddr};
//use pnet::datalink::{Channel, MacAddr, NetworkInterface};

use pnet::packet::arp::ArpPacket;
//use pnet::packet::arp::{ArpHardwareTypes, ArpOperations};
//use pnet::packet::arp::{ArpPacket, MutableArpPacket};
use pnet::packet::ethernet::{EtherTypes, EthernetPacket};
use pnet::packet::icmp::{echo_reply, echo_request, IcmpPacket, IcmpTypes};
use pnet::packet::icmpv6::Icmpv6Packet;
use pnet::packet::ip::{IpNextHeaderProtocol, IpNextHeaderProtocols};
use pnet::packet::ipv4::Ipv4Packet;
use pnet::packet::ipv6::Ipv6Packet;
use pnet::packet::tcp::TcpPacket;
use pnet::packet::udp::UdpPacket;
use pnet::packet::Packet;

//use modbus::modbus_tcp::{ModbusTCPPacket, FunctionFieldValues};
mod modbus_tcp;
use modbus_tcp::*;

// Example Attributes (for logging)
#[derive(Debug)]
pub struct PacketAttr {
    pub interface_name: String,
    pub src_mac: MacAddr,
    pub dst_mac: MacAddr,
    pub src_addr: IpAddr,
    pub dst_addr: IpAddr,
    pub src_port: u16,
    pub dst_port: u16,
    pub length: u32,
    pub transaction: u16,
    pub protocol: u16,
    pub len: u16,
    pub unit_id: u8,
    pub function: u8,
    pub ref_number: u16,
    pub data: u16,
    pub mult_count: u8,
    pub mult_data: String
}

impl PacketAttr {
    pub fn new(
        interface_name: String,
        source_mac: MacAddr,
        destination_mac: MacAddr,
        source: IpAddr,
        destination: IpAddr,
        src_port: u16,
        dst_port: u16,
        length: u32,
    ) -> Self {
        Self {
            interface_name: interface_name,
            src_mac: source_mac,
            dst_mac: destination_mac,
            src_addr: source,
            dst_addr: destination,
            src_port: src_port,
            dst_port: dst_port,
            length: length,
            transaction: 0,
            protocol: 0,
            len: 0,
            unit_id: 0,
            function: 0,
            ref_number: 0,
            data: 0,
            mult_count: 0,
            mult_data: "".to_string()
        }
    }

    pub fn set_modbus(
        &mut self,
        modbus_tcp: &ModbusTCPPacket,
        payload: &[u8]
    ){
        match (self.src_port, self.dst_port) {
            ( _ , 502 ) => { /* (送信元, 送信先) Request */
                match modbus_tcp.get_function() {
                    FunctionFieldValues::ReadCoilStatus => {
                        let m_packet = read_coil_status::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_bit_count();
                    }
                    FunctionFieldValues::ReadInputStatus => {
                        let m_packet = read_input_status::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_bit_count();
                    }
                    FunctionFieldValues::ReadHoldingRegister => {
                        let m_packet = read_holding_register::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_bit_count();
                        
                    }
                    FunctionFieldValues::ReadInputRegister => {
                        let m_packet = read_input_register::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_bit_count();
                    }
                    FunctionFieldValues::ForceSingleCoil => {
                        let m_packet = force_single_coil::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_data();
                    }
                    FunctionFieldValues::PresetSingleRegister => {
                        let m_packet = preset_single_register::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_data();
                    }
                    FunctionFieldValues::Diagnostics => {
                        let m_packet = diagnostics::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                    }
                    FunctionFieldValues::FetchCommunicationEventCounter  => {
                        let m_packet = fetch_communication_event_counter::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                    }
                    FunctionFieldValues::FetchCommunicationEventCounterLog  => {
                        let m_packet = fetch_communication_event_counter_log::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                    }
                    FunctionFieldValues::ForceMultipleCoils => {
                        let m_packet = force_multiple_coils::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_register_count();
                        self.mult_count = m_packet.get_byte_count();
                        let datas = m_packet.get_data();
                        for data in datas{
                            self.mult_data += &reverse_string(&format!("{:0>8b}", data));
                        }
                    }
                    FunctionFieldValues::PresetMultipleRegisters => {
                        let m_packet = preset_multiple_registers::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_register_count();
                        self.mult_count = m_packet.get_byte_count();
                        let datas = m_packet.get_data();
                        for data in datas{
                            self.mult_data += &reverse_string(&format!("{:0>8b}", data));
                        }
                    }
                    FunctionFieldValues::ReportSlaveID  => {
                        let m_packet = report_slave_id::request::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                    }
                    _ => {
                        
                    }
                }
            }
            ( 502 , _ ) => { /* (送信元, 送信先) Reply */
                match modbus_tcp.get_function() {
                    FunctionFieldValues::ReadCoilStatus => {
                        let m_packet = read_coil_status::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.mult_count = m_packet.get_byte_count();
                        let datas = m_packet.get_data();
                        for data in datas{
                            self.mult_data += &reverse_string(&format!("{:0>8b}", data));
                        }
                    }
                    FunctionFieldValues::ReadInputStatus => {
                        let m_packet = read_input_status::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.mult_count = m_packet.get_byte_count();
                        let datas = m_packet.get_data();
                        for data in datas{
                            self.mult_data += &reverse_string(&format!("{:0>8b}", data));
                        }
                    }
                    FunctionFieldValues::ReadHoldingRegister => {
                        let m_packet = read_holding_register::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.mult_count = m_packet.get_byte_count();
                        let datas = m_packet.get_data();
                        for data in datas{
                            self.mult_data += &reverse_string(&format!("{:0>8b}", data));
                        }
                    }
                    FunctionFieldValues::ReadInputRegister => {
                        let m_packet = read_input_register::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.mult_count = m_packet.get_byte_count();
                        let datas = m_packet.get_data();
                        for data in datas{
                            self.mult_data += &reverse_string(&format!("{:0>8b}", data));
                        }
                    }
                    FunctionFieldValues::ForceSingleCoil => {
                        let m_packet = force_single_coil::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_data();
                    }
                    FunctionFieldValues::PresetSingleRegister => {
                        let m_packet = preset_single_register::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_data();
                    }
                    FunctionFieldValues::Diagnostics => {
                        let m_packet = diagnostics::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                    }
                    FunctionFieldValues::FetchCommunicationEventCounter  => {
                        let m_packet = fetch_communication_event_counter::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                    }
                    FunctionFieldValues::FetchCommunicationEventCounterLog  => {
                        let m_packet = fetch_communication_event_counter_log::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                    }
                    FunctionFieldValues::ForceMultipleCoils => {
                        let m_packet = force_multiple_coils::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_data();
                    }
                    FunctionFieldValues::PresetMultipleRegisters => {
                        let m_packet = preset_multiple_registers::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                        self.ref_number = m_packet.get_reference_number();
                        self.data = m_packet.get_data();
                    }
                    FunctionFieldValues::ReportSlaveID  => {
                        let m_packet = report_slave_id::reply::ModbusPacket::new(payload).unwrap();
                        self.transaction = m_packet.get_transaction();
                        self.protocol = m_packet.get_protocol();
                        self.len = m_packet.get_length();
                        self.unit_id = m_packet.get_unit();
                        self.function = m_packet.get_function();
                    }
                    _ => {
                        
                    }
                }
            }
            ( _ , _ ) => { /* ModbusTCP以外の通信 */ }
        }
    }

    pub fn clone(&self) -> PacketAttr{
        let mut cp = PacketAttr::new(
            self.interface_name.clone(),
            self.src_mac.clone(),
            self.dst_mac.clone(),
            self.src_addr.clone(),
            self.dst_addr.clone(),
            self.src_port.clone(),
            self.dst_port.clone(),
            self.length.clone(),
        );
        cp.transaction = self.transaction.clone();
        cp.protocol = self.protocol.clone();
        cp.len = self.len.clone();
        cp.unit_id = self.unit_id.clone();
        cp.function = self.function.clone();
        cp.ref_number = self.ref_number.clone();
        cp.data = self.data.clone();
        cp.mult_count = self.mult_count.clone();
        cp.mult_data = self.mult_data.clone();
        cp
    }
}

pub enum Action {
    Accept(String),
    Log(PacketAttr),
    Drop(String),
}

fn reverse_string(input: &String) -> String {
    let mut reversed = String::new();
    let mut chars: Vec<char> = Vec::new();

    for c in input.chars().into_iter() {
        chars.push(c);
    }

    for i in (0..chars.len()).rev() {
        reversed += &chars[i].to_string();
    }

    return reversed;
}

fn get_type<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

fn handle_udp_packet(
    interface_name: &str,
    source_mac: MacAddr,
    destination_mac: MacAddr,
    source: IpAddr,
    destination: IpAddr,
    packet: &[u8],
) -> Option<Action> {
    let udp = UdpPacket::new(packet);

    if let Some(udp) = udp {
        let message = format!(
            "[{}]: UDP Packet: {}:{} > {}:{}; length: {}",
            interface_name,
            source,
            udp.get_source(),
            destination,
            udp.get_destination(),
            udp.get_length()
        );
        log::debug!("{}", message);
        /*
        let packet_attr = PacketAttr::new(
            interface_name,
            source_mac,
            destination_mac,
            source,
            destination,
            &udp.get_source(),
            &udp.get_destination(),
            &(packet.len() as u32)
        );*/
        //return Some(Action::Log(packet_attr));
        return Some(Action::Accept(message));
    } else {
        log::error!("[{}]: Malformed UDP Packet", interface_name);
        return None;
    }
}

fn handle_icmp_packet(
    interface_name: &str,
    source: IpAddr,
    destination: IpAddr,
    packet: &[u8],
) -> Option<Action> {
    let icmp_packet = IcmpPacket::new(packet);
    if let Some(icmp_packet) = icmp_packet {
        //println!("icmp_packet: {}", get_type(&icmp_packet));
        match icmp_packet.get_icmp_type() {
            IcmpTypes::EchoReply => {
                let echo_reply_packet = echo_reply::EchoReplyPacket::new(packet).unwrap();
                let message = format!(
                    "[{}]: ICMP echo reply {} -> {} (seq={:?}, id={:?})",
                    interface_name,
                    source,
                    destination,
                    echo_reply_packet.get_sequence_number(),
                    echo_reply_packet.get_identifier()
                );
                log::debug!("{}", message);
                return Some(Action::Accept(message));
            }
            IcmpTypes::EchoRequest => {
                let echo_request_packet = echo_request::EchoRequestPacket::new(packet).unwrap();
                let message = format!(
                    "[{}]: ICMP echo request {} -> {} (seq={:?}, id={:?})",
                    interface_name,
                    source,
                    destination,
                    echo_request_packet.get_sequence_number(),
                    echo_request_packet.get_identifier()
                );
                log::debug!("{}", message);
                return Some(Action::Accept(message));
            }
            _ => {
                let message = format!(
                    "[{}]: ICMP packet {} -> {} (type={:?})",
                    interface_name,
                    source,
                    destination,
                    icmp_packet.get_icmp_type()
                );
                log::debug!("{}", message);
                return Some(Action::Accept(message));
            }
        }
    } else {
        log::error!("[{}]: Malformed ICMP Packet", interface_name);
        return None;
    }
}

fn handle_icmpv6_packet(
    interface_name: &str,
    source: IpAddr,
    destination: IpAddr,
    packet: &[u8],
) -> Option<Action> {
    let icmpv6_packet = Icmpv6Packet::new(packet);
    if let Some(icmpv6_packet) = icmpv6_packet {
        let message = format!(
            "[{}]: ICMPv6 packet {} -> {} (type={:?})",
            interface_name,
            source,
            destination,
            icmpv6_packet.get_icmpv6_type()
        );
        log::debug!("{}", message);
        return Some(Action::Accept(message));
    } else {
        log::error!("[{}]: Malformed ICMPv6 Packet", interface_name);
        return None;
    }
}

fn handle_tcp_packet(
    interface_name: &str,
    source_mac: MacAddr,
    destination_mac: MacAddr,
    source: IpAddr,
    destination: IpAddr,
    packet: &[u8],
) -> Option<Action> {
    let tcp = TcpPacket::new(packet);
    if let Some(tcp) = tcp {
        let message = format!(
            "[{}]: TCP Packet: {}:{} > {}:{}; length: {}",
            interface_name,
            source,
            tcp.get_source(),
            destination,
            tcp.get_destination(),
            packet.len()
        );
        log::debug!("{}", message);
        let modbus_tcp = ModbusTCPPacket::new(tcp.payload());
        if let Some(modbus_tcp) = modbus_tcp{
            let mut packet_attr = PacketAttr::new(
                interface_name.to_string(),
                source_mac,
                destination_mac,
                source,
                destination,
                tcp.get_source().clone(),
                tcp.get_destination().clone(),
                (packet.len() as u32)
            );
            packet_attr.set_modbus(&modbus_tcp, &tcp.payload());
            return Some(Action::Log(packet_attr))
        }
        return Some(Action::Accept(message));
    } else {
        log::error!("[{}]: Malformed TCP Packet", interface_name);
        return None;
    }
}

fn handle_transport_protocol(
    interface_name: &str,
    source_mac: MacAddr,
    destination_mac: MacAddr,
    source: IpAddr,
    destination: IpAddr,
    protocol: IpNextHeaderProtocol,
    packet: &[u8],
) -> Option<Action> {
    match protocol {
        IpNextHeaderProtocols::Udp => {
            handle_udp_packet(interface_name, source_mac, destination_mac, source, destination, packet)
        }
        IpNextHeaderProtocols::Tcp => {
            handle_tcp_packet(interface_name, source_mac, destination_mac, source, destination, packet)
        }
        IpNextHeaderProtocols::Icmp => {
            handle_icmp_packet(interface_name, source, destination, packet)
        }
        IpNextHeaderProtocols::Icmpv6 => {
            handle_icmpv6_packet(interface_name, source, destination, packet)
        }
        _ => {
            log::error!(
                "[{}]: Unknown {} packet: {} > {}; protocol: {:?} length: {}",
                interface_name,
                match source {
                    IpAddr::V4(..) => "IPv4",
                    _ => "IPv6",
                },
                source,
                destination,
                protocol,
                packet.len()
            );
            return None;
        }
    }
}

fn handle_ipv4_packet(interface_name: &str, ethernet: &EthernetPacket) -> Option<Action> {
    let header = Ipv4Packet::new(ethernet.payload());
    if let Some(header) = header {
        handle_transport_protocol(
            interface_name,
            ethernet.get_source(),
            ethernet.get_destination(),
            IpAddr::V4(header.get_source()),
            IpAddr::V4(header.get_destination()),
            header.get_next_level_protocol(),
            header.payload(),
        )
    } else {
        log::error!("[{}]: Malformed IPv4 Packet", interface_name);
        return None;
    }
}

fn handle_ipv6_packet(interface_name: &str, ethernet: &EthernetPacket) -> Option<Action> {
    let header = Ipv6Packet::new(ethernet.payload());
    if let Some(header) = header {
        handle_transport_protocol(
            interface_name,
            ethernet.get_source(),
            ethernet.get_destination(),
            IpAddr::V6(header.get_source()),
            IpAddr::V6(header.get_destination()),
            header.get_next_header(),
            header.payload(),
        )
    } else {
        log::error!("[{}]: Malformed IPv6 Packet", interface_name);
        return None;
    }
}

fn handle_arp_packet(interface_name: &str, ethernet: &EthernetPacket) -> Option<Action> {
    let header = ArpPacket::new(ethernet.payload());
    if let Some(header) = header {
        let message = format!(
            "[{}]: ARP packet: {}({}) > {}({}); operation: {:?}",
            interface_name,
            ethernet.get_source(),
            header.get_sender_proto_addr(),
            ethernet.get_destination(),
            header.get_target_proto_addr(),
            header.get_operation()
        );
        log::debug!("{}", message);
        return Some(Action::Accept(message));
    } else {
        log::error!("[{}]: Malformed ARP Packet", interface_name);
        return None;
    }
}

pub fn handle_ethernet_frame(
    interface: &NetworkInterface,
    ethernet: &EthernetPacket,
) -> Option<Action> {
    let interface_name = &interface.name[..];
    match ethernet.get_ethertype() {
        EtherTypes::Ipv4 => handle_ipv4_packet(interface_name, ethernet),
        EtherTypes::Ipv6 => handle_ipv6_packet(interface_name, ethernet),
        EtherTypes::Arp => handle_arp_packet(interface_name, ethernet),
        _ => {
            log::error!(
                "[{}]: Unknown packet: {} > {}; ethertype: {:?} length: {}",
                interface_name,
                ethernet.get_source(),
                ethernet.get_destination(),
                ethernet.get_ethertype(),
                ethernet.packet().len()
            );
            return None;
        }
    }
}
