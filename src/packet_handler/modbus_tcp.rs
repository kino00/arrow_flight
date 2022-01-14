use pnet_macros::packet;
use pnet_macros_support::types::*;
use pnet_macros_support::packet::PrimitiveValues;

#[packet]
pub struct ModbusTCP {
    pub transaction: u16be,
    pub protocol: u16be,
    pub length: u16be,
    pub unit: u8,
    #[construct_with(u8)]
    pub function: FunctionField,
    #[payload]
    pub payload: Vec<u8>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash)]
pub struct FunctionField(pub u8);

impl FunctionField {
    pub fn new(field_val: u8) -> FunctionField {
        FunctionField(field_val)
    }
}

impl PrimitiveValues for FunctionField {
    type T = (u8,);
    fn to_primitive_values(&self) -> (u8,) {
        (self.0,)
    }
}

#[allow(non_snake_case)]
#[allow(non_upper_case_globals)]
#[allow(dead_code)]
pub mod FunctionFieldValues {
    use super::FunctionField;

    pub const ReadCoilStatus: FunctionField = FunctionField(1);
    pub const ReadInputStatus: FunctionField = FunctionField(2);
    pub const ReadHoldingRegister: FunctionField = FunctionField(3);
    pub const ReadInputRegister: FunctionField = FunctionField(4);
    pub const ForceSingleCoil: FunctionField = FunctionField(5);
    pub const PresetSingleRegister: FunctionField = FunctionField(6);
    pub const Diagnostics: FunctionField = FunctionField(8);
    pub const FetchCommunicationEventCounter: FunctionField = FunctionField(11);
    pub const FetchCommunicationEventCounterLog: FunctionField = FunctionField(12);
    pub const ForceMultipleCoils: FunctionField = FunctionField(15);
    pub const PresetMultipleRegisters: FunctionField = FunctionField(16);
    pub const ReportSlaveID: FunctionField = FunctionField(17);
}

pub mod read_coil_status {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |           Bit Count           |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);
    
        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }
    
        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct BitCount(pub u16);
    
        impl BitCount {
            #[allow(dead_code)]
            pub fn new(val: u16) -> BitCount {
                BitCount(val)
            }
        }
    
        impl PrimitiveValues for BitCount {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub bit_count: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |   Byte Count  |   Data ...
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ByteCount(pub u8);

        impl ByteCount {
            #[allow(dead_code)]
            pub fn new(val: u8) -> ByteCount {
                ByteCount(val)
            }
        }

        impl PrimitiveValues for ByteCount {
            type T = (u8,);
            fn to_primitive_values(&self) -> (u8,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub byte_count: u8,
            #[length_fn = "data_length_b"]
            pub data: Vec<u8>,
            #[payload]
            pub payload: Vec<u8>,
        }

        #[inline]
        fn data_length_b(modbus: &ModbusPacket) -> usize {
            let byte_count = modbus.get_byte_count();

            byte_count as usize
        }
    }
}

pub mod read_input_status {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |           Bit Count           |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);

        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }

        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct BitCount(pub u16);

        impl BitCount {
            #[allow(dead_code)]
            pub fn new(val: u16) -> BitCount {
                BitCount(val)
            }
        }

        impl PrimitiveValues for BitCount {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub bit_count: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |   Byte Count  |   Data ...
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ByteCount(pub u8);
    
        impl ByteCount {
            #[allow(dead_code)]
            pub fn new(val: u8) -> ByteCount {
                ByteCount(val)
            }
        }
    
        impl PrimitiveValues for ByteCount {
            type T = (u8,);
            fn to_primitive_values(&self) -> (u8,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub byte_count: u8,
            #[length_fn = "data_length_b"]
            pub data: Vec<u8>,
            #[payload]
            pub payload: Vec<u8>,
        }
    
        #[inline]
        fn data_length_b(modbus: &ModbusPacket) -> usize {
            let byte_count = modbus.get_byte_count();
    
            byte_count as usize
        }
    }
}

pub mod read_holding_register {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |           Bit Count           |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);

        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }

        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct BitCount(pub u16);

        impl BitCount {
            #[allow(dead_code)]
            pub fn new(val: u16) -> BitCount {
                BitCount(val)
            }
        }

        impl PrimitiveValues for BitCount {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub bit_count: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |   Byte Count  |   Data ...
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ByteCount(pub u8);
    
        impl ByteCount {
            #[allow(dead_code)]
            pub fn new(val: u8) -> ByteCount {
                ByteCount(val)
            }
        }
    
        impl PrimitiveValues for ByteCount {
            type T = (u8,);
            fn to_primitive_values(&self) -> (u8,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub byte_count: u8,
            #[length_fn = "data_length_b"]
            pub data: Vec<u8>,
            #[payload]
            pub payload: Vec<u8>,
        }
    
        #[inline]
        fn data_length_b(modbus: &ModbusPacket) -> usize {
            let byte_count = modbus.get_byte_count();
    
            byte_count as usize
        }
    }
}

pub mod read_input_register {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |           Bit Count           |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);

        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }

        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct BitCount(pub u16);

        impl BitCount {
            #[allow(dead_code)]
            pub fn new(val: u16) -> BitCount {
                BitCount(val)
            }
        }

        impl PrimitiveValues for BitCount {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub bit_count: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |   Byte Count  |   Data ...
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ByteCount(pub u8);
    
        impl ByteCount {
            #[allow(dead_code)]
            pub fn new(val: u8) -> ByteCount {
                ByteCount(val)
            }
        }
    
        impl PrimitiveValues for ByteCount {
            type T = (u8,);
            fn to_primitive_values(&self) -> (u8,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub byte_count: u8,
            #[length_fn = "data_length_b"]
            pub data: Vec<u8>,
            #[payload]
            pub payload: Vec<u8>,
        }
    
        #[inline]
        fn data_length_b(modbus: &ModbusPacket) -> usize {
            let byte_count = modbus.get_byte_count();
    
            byte_count as usize
        }
    }
}

pub mod force_single_coil {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |             data              |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);
    
        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }
    
        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Data(pub u16);
    
        impl Data {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Data {
                Data(val)
            }
        }
    
        impl PrimitiveValues for Data {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub data: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |             data              |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);
    
        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }
    
        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Data(pub u16);
    
        impl Data {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Data {
                Data(val)
            }
        }
    
        impl PrimitiveValues for Data {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub data: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }
}

pub mod preset_single_register {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |             data              |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);

        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }

        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Data(pub u16);

        impl Data {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Data {
                Data(val)
            }
        }

        impl PrimitiveValues for Data {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub data: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |             data              |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);

        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }

        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Data(pub u16);

        impl Data {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Data {
                Data(val)
            }
        }

        impl PrimitiveValues for Data {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub data: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }
}

pub mod diagnostics {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            sub code           |             data              |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct SubCode(pub u16);
    
        impl SubCode {
            #[allow(dead_code)]
            pub fn new(val: u16) -> SubCode {
                SubCode(val)
            }
        }
    
        impl PrimitiveValues for SubCode {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Data(pub u16);
    
        impl Data {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Data {
                Data(val)
            }
        }
    
        impl PrimitiveValues for Data {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub sub_code: u16be,
            pub data: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //use super::super::packet_d;
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            sub code           |             data              |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        //use super::FunctionField;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct SubCode(pub u16);
    
        impl SubCode {
            #[allow(dead_code)]
            pub fn new(val: u16) -> SubCode {
                SubCode(val)
            }
        }
    
        impl PrimitiveValues for SubCode {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Data(pub u16);
    
        impl Data {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Data {
                Data(val)
            }
        }
    
        impl PrimitiveValues for Data {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        /*
        #[allow(non_snake_case)]
        #[allow(non_upper_case_globals)]
        pub mod FunctionFieldValues {
            use super::FunctionField;
        
            pub const Diagnostics: FunctionField = FunctionField(8);
        }
        */

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            //#[construct_with(u8)]
            //pub function: FunctionField,
            pub function: u8,
            pub sub_code: u16be,
            pub data: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }
}

pub mod fetch_communication_event_counter {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            //#[construct_with(u8)]
            //pub function: FunctionField,
            pub function: u8,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |             status            |        event counter          |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Status(pub u16);

        impl Status {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Status {
                Status(val)
            }
        }

        impl PrimitiveValues for Status {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct EventCounter(pub u16);

        impl EventCounter {
            #[allow(dead_code)]
            pub fn new(val: u16) -> EventCounter {
                EventCounter(val)
            }
        }

        impl PrimitiveValues for EventCounter {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub status: u16be,
            pub event_counter: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }
}

pub mod fetch_communication_event_counter_log {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |   byte count  |             status            | evnet counter  
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //!  event counter  |       message counter         |     event ....
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ByteCount(pub u8);
    
        impl ByteCount {
            #[allow(dead_code)]
            pub fn new(val: u8) -> ByteCount {
                ByteCount(val)
            }
        }
    
        impl PrimitiveValues for ByteCount {
            type T = (u8,);
            fn to_primitive_values(&self) -> (u8,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Status(pub u16);
    
        impl Status {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Status {
                Status(val)
            }
        }
    
        impl PrimitiveValues for Status {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct EventCounter(pub u16);
    
        impl EventCounter {
            #[allow(dead_code)]
            pub fn new(val: u16) -> EventCounter {
                EventCounter(val)
            }
        }
    
        impl PrimitiveValues for EventCounter {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct MessageCounter(pub u16);
    
        impl MessageCounter {
            #[allow(dead_code)]
            pub fn new(val: u16) -> MessageCounter {
                MessageCounter(val)
            }
        }
    
        impl PrimitiveValues for MessageCounter {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub byte_count: u8,
            pub status: u16be,
            pub event_counter: u16be,
            pub message_counter: u16be,
            #[length_fn = "data_length_g" ]
            pub data: Vec<u8>,
            #[payload]
            pub payload: Vec<u8>,
        }
    
        #[inline]
        fn data_length_g(modbus: &ModbusPacket) -> usize {
            let byte_count = modbus.get_byte_count();
    
            byte_count as usize - 6
        }
    }
}

pub mod force_multiple_coils {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |        Register Count         |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |  byte count   |             data       ....
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);
    
        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }
    
        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct RegisterCount(pub u16);
    
        impl RegisterCount {
            #[allow(dead_code)]
            pub fn new(val: u16) -> RegisterCount {
                RegisterCount(val)
            }
        }
    
        impl PrimitiveValues for RegisterCount {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ByteCount(pub u8);
    
        impl ByteCount {
            #[allow(dead_code)]
            pub fn new(val: u8) -> ByteCount {
                ByteCount(val)
            }
        }
    
        impl PrimitiveValues for ByteCount {
            type T = (u8,);
            fn to_primitive_values(&self) -> (u8,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub register_count: u16be,
            pub byte_count: u8,
            #[length_fn = "data_length_h" ]
            pub data: Vec<u8>,
            #[payload]
            pub payload: Vec<u8>,
        }
    
        #[inline]
        fn data_length_h(modbus: &ModbusPacket) -> usize {
            let byte_count = modbus.get_byte_count();
    
            byte_count as usize
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |             data              |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);
    
        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }
    
        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Data(pub u16);
    
        impl Data {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Data {
                Data(val)
            }
        }
    
        impl PrimitiveValues for Data {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub data: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }
}

pub mod preset_multiple_registers {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |        Register Count         |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |  byte count   |             data       ....
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);
    
        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }
    
        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct RegisterCount(pub u16);
    
        impl RegisterCount {
            #[allow(dead_code)]
            pub fn new(val: u16) -> RegisterCount {
                RegisterCount(val)
            }
        }
    
        impl PrimitiveValues for RegisterCount {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ByteCount(pub u8);
    
        impl ByteCount {
            #[allow(dead_code)]
            pub fn new(val: u8) -> ByteCount {
                ByteCount(val)
            }
        }
    
        impl PrimitiveValues for ByteCount {
            type T = (u8,);
            fn to_primitive_values(&self) -> (u8,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub register_count: u16be,
            pub byte_count: u8,
            #[length_fn = "data_length_h" ]
            pub data: Vec<u8>,
            #[payload]
            pub payload: Vec<u8>,
        }
    
        #[inline]
        fn data_length_h(modbus: &ModbusPacket) -> usize {
            let byte_count = modbus.get_byte_count();
    
            byte_count as usize
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |        Reference Number       |             data              |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros_support::packet::PrimitiveValues;
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct ReferenceNumber(pub u16);
    
        impl ReferenceNumber {
            #[allow(dead_code)]
            pub fn new(val: u16) -> ReferenceNumber {
                ReferenceNumber(val)
            }
        }
    
        impl PrimitiveValues for ReferenceNumber {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }
    
        #[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct Data(pub u16);
    
        impl Data {
            #[allow(dead_code)]
            pub fn new(val: u16) -> Data {
                Data(val)
            }
        }
    
        impl PrimitiveValues for Data {
            type T = (u16,);
            fn to_primitive_values(&self) -> (u16,) {
                (self.0,)
            }
        }

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            pub reference_number: u16be,
            pub data: u16be,
            #[payload]
            pub payload: Vec<u8>,
        }
    }
}

pub mod report_slave_id {
    pub mod request {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros::packet;
        use pnet_macros_support::types::*;

        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            #[payload]
            pub payload: Vec<u8>,
        }
    }

    pub mod reply {
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |          Transaction          |           Protocol            |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        //! |            Length             |      Unit     |   Function    |
        //! +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    
        use pnet_macros::packet;
        use pnet_macros_support::types::*;
    
        #[packet]
        pub struct Modbus {
            pub transaction: u16be,
            pub protocol: u16be,
            pub length: u16be,
            pub unit: u8,
            pub function: u8,
            #[payload]
            pub payload: Vec<u8>,
        }
    }
}
