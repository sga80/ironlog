const INVALID_REQUEST_TYPE: &str = "invalid request type";
const INVALID_PAYLOAD_TYPE: &str = "invalid payload type";

const INVALID_WRITE_STATUS_TYPE: &str = "invalid write status type";

#[repr(u16)] // represent as u16 in memory as Request type is 2 bytes
#[derive(Clone, Debug, Copy)]
pub enum RequestType {
    Produce = 1,
    Fetch = 2,
    Ack = 3,
}
impl TryFrom<u16> for RequestType {
    type Error = std::io::Error;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Produce),
            2 => Ok(Self::Fetch),
            3 => Ok(Self::Ack),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, INVALID_REQUEST_TYPE))
        }
    }
}
#[repr(u16)] // represent as u16 in memory as Request type is 2 bytes
#[derive(Clone, Debug, Copy, Default)]
pub enum PayloadType {
    #[default]
    Text = 1,
    Json = 2,
    Binary = 3,
}


impl TryFrom<u16> for PayloadType {
    type Error = std::io::Error;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Text),
            2 => Ok(Self::Json),
            3 => Ok(Self::Binary),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, INVALID_PAYLOAD_TYPE))
        }
    }
}

#[repr(u8)] // represent as u8 in memory as WriteStatus which is 1 byte
#[derive(Clone, Debug, Copy, PartialEq)]
pub enum WriteStatus {
    Success = 0,
    Failure = 1,
}

impl TryFrom<u8> for WriteStatus {
    type Error = std::io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Success),
            1 => Ok(Self::Failure),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, INVALID_WRITE_STATUS_TYPE))
        }
    }
}
