use std::{convert::TryInto};
use std::error::Error;
use tokio::{io::AsyncReadExt, net::TcpStream};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncWriteExt, BufWriter};

pub const DF_SCALAR: i16 = 0;
pub const DF_VECTOR: i16 = 1;
pub const DF_PAIR: i16 = 2;
pub const DF_MATRIX: i16 = 3;
pub const DF_SET: i16 = 4;
pub const DF_DICTIONARY: i16 = 5;
pub const DF_TABLE: i16 = 6;
pub const DF_CHART: i16 = 7;
pub const DF_CHUNK: i16 = 8;
pub const MAX_FORM_VALUE: i16 = 9;

pub const DT_VOID: i16 = 0;
pub const DT_BOOL: i16 = 1;
pub const DT_CHAR: i16 = 2;
pub const DT_SHORT: i16 = 3;
pub const DT_INT: i16 = 4;
pub const DT_LONG: i16 = 5;
pub const DT_DATE: i16 = 6;
pub const DT_MONTH: i16 = 7;
pub const DT_TIME: i16 = 8;
pub const DT_MINUTE: i16 = 9;
pub const DT_SECOND: i16 = 10;
pub const DT_DATETIME: i16 = 11;
pub const DT_TIMESTAMP: i16 = 12;
pub const DT_NANOTIME: i16 = 13;
pub const DT_NANOTIMESTAMP: i16 = 14;
pub const DT_FLOAT: i16 = 15;
pub const DT_DOUBLE: i16 = 16;
pub const DT_SYMBOL: i16 = 17;
pub const DT_STRING: i16 = 18;
pub const DT_UUID: i16 = 19;
pub const DT_FUNCTIONDEF: i16 = 20;
pub const DT_HANDLE: i16 = 21;
pub const DT_CODE: i16 = 22;
pub const DT_DATASOURCE: i16 = 23;
pub const DT_RESOURCE: i16 = 24;
pub const DT_ANY: i16 = 25;
pub const DT_COMPRESS: i16 = 26;
pub const DT_DICTIONARY: i16 = 27;
pub const DT_DATEHOUR: i16 = 28;
pub const DT_DATEMINUTE: i16 = 29;
pub const DT_IP: i16 = 30;
pub const DT_INT128: i16 = 31;
pub const DT_OBJECT: i16 = 32;
pub const MAX_TYPE_VALUE: i16 = 33;

pub const CUM_MONTH_DAYS: [i32; 13] = [0,31,59,90,120,151,181,212,243,273,304,334,365];
pub const CUM_LEAP_MONTH_DAYS: [i32;13] = [0,31,60,91,121,152,182,213,244,274,305,335,366];
pub const MONTH_DAYS: [i32; 12] = [31,28,31,30,31,30,31,31,30,31,30,31];
pub const LEAP_MONTH_DAYS: [i32; 12] = [31,29,31,30,31,30,31,31,30,31,30,31];

#[derive(Debug)]
struct DolphinDBError {
    msg: String,
}

impl DolphinDBError {
    fn create(msg: &str) -> Box<dyn Error> {
        Box::new(DolphinDBError {
            msg: msg.to_owned(),
        })
    }
}

impl std::fmt::Display for DolphinDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Error for DolphinDBError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

#[derive(Debug)]
pub struct DBConnection {
    conn: TcpClient,
    session_id: String,
    little_endian: bool
}

impl DBConnection {
    pub async fn new(ip: String) -> DBConnection {
        DBConnection {
            conn: TcpClient::init(ip).await,
            session_id: "".to_string(),
            little_endian: false,
        }
    }

    pub async fn connect(&mut self, username: String, passwd: String) {
        self.conn.write("API 0 8\nconnect\n").await;
        self.conn.flush().await;

        let msg = self.conn.read_line().await;
        let split_data: Vec<&str> = msg.split(" ").collect();
        let session_id = split_data[0];
        if !split_data[2].eq("0") {
            self.little_endian = true;
            self.conn.set_little_endian(true);
        }
        self.session_id = session_id.to_string();
        self.conn.read_line().await; // OK

        let mut args = Vec::new();
        args.push(DataValue::String(username));
        args.push(DataValue::String(passwd));
        self.run_func("login", args).await.unwrap();
    }

    pub async fn run(&mut self, s: &str) -> Result<DataValue, Box<dyn Error>> {
        let mut is_urgent_cancel_job = false;
        if s.starts_with("cancelJob(") || s.starts_with("cancelConsoleJob(") {
            is_urgent_cancel_job = true;
        }

        let mut body = format!("script\n{}", s);
        if is_urgent_cancel_job {
            body = format!("{} / 1_1_8_8\n", body);
        }

        let message = format!("API {} {}\n{}", self.session_id, body.len(), body);
        self.conn.write(&message).await;
        self.conn.flush().await;

        let mut header = self.conn.read_line().await;
        while header.eq("MSG") {
            /*let msg = in.readString();
            if (listener != null)
                listener.progress(msg);*/
            header = self.conn.read_line().await;
        }

        let headers: Vec<&str> = header.split(" ").collect();
        if headers.len() != 3 {
            return Err(DolphinDBError::create(&format!("Received invalid header: {}", header)));
        }

        let num_object = headers[1].parse::<i32>().unwrap();

        let msg = self.conn.read_line().await;
        if !msg.eq("OK") {
            return Err(DolphinDBError::create(&format!("DBErr {}", msg)));
        }

        if num_object == 0 {
            return Err(DolphinDBError::create("Null!"));
        }

        let flag = self.conn.read_short().await;
        let num_form = flag >> 8;
        let num_type = flag & 0xff;

        //debug!("11 FORM:{} TYPE:{}", num_form, num_type);

        if num_form < 0 || num_form > MAX_FORM_VALUE {
            return Err(DolphinDBError::create(&format!("Invalid form value: {}", num_form)));
        }

        if num_type < 0 || num_type > MAX_TYPE_VALUE {
            return Err(DolphinDBError::create(&format!("Invalid type value: {}", num_type)));
        }

        //if(fetchSize > 0 && df == Entity.DATA_FORM.DF_VECTOR && dt == Entity.DATA_TYPE.DT_ANY){
        //    return new EntityBlockReader(in);
        //}

        return Ok(self.parse_result(num_form, num_type).await);
    }

    pub async fn run_func(&mut self, s: &str, args: Vec<DataValue>) -> Result<DataValue, Box<dyn Error>> {
        let mut body = format!("function\n{}", s);
        body = format!("{}\n{}\n", body, args.len());
        body = format!("{}{}", body, if self.little_endian {
            1
        } else {
            0
        });

        let message = format!("API {} {}\n{}", self.session_id, body.len(), body);
        self.conn.write(&message).await;
        for v in args {
            BasicValue::encode(&mut self.conn, &v).await;
        }
        self.conn.flush().await;

        let header = self.conn.read_line().await;
        let headers: Vec<&str> = header.split(" ").collect();
        if headers.len() != 3 {
            return Err(DolphinDBError::create(&format!("Received invalid header: {}", header)));
        }

        let num_object = headers[1].parse::<i32>().unwrap();

        let msg = self.conn.read_line().await;
        if !msg.eq("OK") {
            return Err(DolphinDBError::create(&format!("NO OK {}", msg)));
        }

        if num_object == 0 {
            return Err(DolphinDBError::create("Null!"));
        }

        let flag = self.conn.read_short().await;
        let num_form = flag >> 8;
        let num_type = flag & 0xff;

        //debug!("11 FORM:{} TYPE:{}", num_form, num_type);

        if num_form < 0 || num_form > MAX_FORM_VALUE {
            return Err(DolphinDBError::create(&format!("Invalid form value: {}", num_form)));
        }

        if num_type < 0 || num_type > MAX_TYPE_VALUE {
            return Err(DolphinDBError::create(&format!("Invalid type value: {}", num_type)));
        }

        //if(fetchSize > 0 && df == Entity.DATA_FORM.DF_VECTOR && dt == Entity.DATA_TYPE.DT_ANY){
        //    return new EntityBlockReader(in);
        //}

        return Ok(self.parse_result(num_form, num_type).await);
    }

    async fn parse_result(&mut self, num_form: i16, num_type: i16) -> DataValue {
        //println!("{} {}", num_form, num_type);

        /*if num_form == DF_TABLE {
            //return new BasicTable(in);
        } else if num_form == DF_CHART {
            //return new BasicChart(in);
        } else if num_form == DF_DICTIONARY {
            //return new BasicDictionary(type, in);
        } else if num_form == DF_SET {
            //return new BasicSet(type, in);
        } else if num_form == DF_CHUNK {
            //return new BasicChunkMeta(in);
        } else if num_type == DT_ANY && num_form == DF_VECTOR {
            //return new BasicAnyVector(in);
        } else if num_type == DT_VOID && num_form == DF_SCALAR {
            //in.readBoolean();
			//return new Void();
        } else {
			/*int index = type.ordinal();
			if(factories[index] == null)
				throw new IOException("Data type " + type.name() +" is not supported yet.");
			else if(form == Entity.DATA_FORM.DF_VECTOR)
				return factories[index].createVector(in);
			else if(form == Entity.DATA_FORM.DF_SCALAR)
				return factories[index].createScalar(in);
			else if(form == Entity.DATA_FORM.DF_MATRIX)
				return factories[index].createMatrix(in);
			else if(form == Entity.DATA_FORM.DF_PAIR)
				return factories[index].createPair(in);
			else
				throw new IOException("Data form " + form.name() +" is not supported yet.");*/
		}*/
        if num_form == DF_TABLE {
            return BasicTable::decode(&mut self.conn).await;
        } else if num_form == DF_SCALAR {
            return BasicValue::decode(&mut self.conn, num_type).await;
        }

        return DataValue::Void();
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    name: String,
    cols: Vec<TableColumn>,
}

impl Table {
    pub fn new() -> Table {
        Table{
            name: "".to_string(),
            cols: Vec::new(),
        }
    }

    pub fn add_col(&mut self, name: String, v: Vector) {
        self.cols.push(TableColumn{
            name,
            num_type: v.num_type,
            data: v,
        })
    }

    pub fn row_len(&self) -> i32 {
        if self.col_len() == 0 {
            return 0;
        }
        return self.cols[0].data.len();
    }

    pub fn col_len(&self) -> i32 {
        return self.cols.len() as i32;
    }

    pub fn get_col(&self) -> &Vec<TableColumn> {
        return &self.cols;
    }
}

#[derive(Debug, Clone)]
pub struct TableColumn {
    name: String,
    num_type: i16,
    data: Vector,
}

impl TableColumn {
    pub fn get_name(&mut self) -> &String {
        return &self.name;
    }

    pub fn get_vector(&mut self) -> &Vector {
        return &self.data;
    }

    pub fn get_num_type(&mut self) -> i16 {
        return self.num_type;
    }
}

#[derive(Debug, Clone)]
pub struct Vector {
    num_type: i16,
    data: Vec<DataValue>,
}

impl Vector {
    pub fn new() -> Vector {
        Vector{
            num_type: -1,
            data: Vec::new(),
        }
    }

    pub fn create(num_type: i16) -> Vector {
        Vector{
            num_type,
            data: Vec::new(),
        }
    }

    pub fn set_type(&mut self, num_type: i16) {
        self.num_type = num_type;
    }

    pub fn push(&mut self, v: DataValue) {
        self.data.push(v);
    }

    pub fn get_num_type(&self) -> i16 {
        return self.num_type;
    }

    pub fn len(&self) -> i32 {
        return self.data.len() as i32;
    }

    pub fn get(&self) -> &Vec<DataValue> {
        return &self.data;
    }
}

#[derive(Debug, Clone)]
pub struct Date {
    pub raw: i32,
    pub year: i32,
    pub month: i32,
    pub day: i32,
}

impl Date {
    pub fn new(year: i32, month: i32, day: i32) -> Date {
        Date {
            raw: 0,
            year,
            month,
            day,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Time {
    pub raw: i32,
    pub hour: i32,
    pub minute: i32,
    pub second: i32,
    pub millisecond: i32,
}

impl Time {
    pub fn new(hour: i32, minute: i32, second: i32, millisecond: i32) -> Time {
        Time {
            raw: 0,
            hour,
            minute,
            second,
            millisecond,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Minute {
    raw: i32,
    hour: i32,
    minute: i32
}

impl Minute {
    pub fn new(hour: i32, minute: i32) -> Minute {
        Minute {
            raw: 0,
            hour,
            minute,
        }
    }
}


#[derive(Debug, Clone)]
pub struct DateTime {
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
}

impl DateTime {
    pub fn new(year: i32, month: i32, day: i32, hour: i32, minute: i32, second: i32) -> DateTime {
        DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
        }
    }

    pub fn to_date(&self) -> Date {
        Date {
            raw: 0,
            year: self.year,
            month: self.month,
            day: self.day,
        }
    }
}


#[derive(Debug)]
struct TcpClient {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    little_endian: bool,
}

impl TcpClient {
    async fn init(ip: String) -> TcpClient {
        let client = TcpStream::connect(ip).await.unwrap();

        return TcpClient {
            stream: BufWriter::new(client),
            buffer: BytesMut::with_capacity(8 * 1024),
            little_endian: false,
        }
    }

    pub fn set_little_endian(&mut self, le: bool) {
        self.little_endian = le;
    }

    async fn flush(&mut self) {
        self.stream.flush().await.unwrap();
    }

    async fn write(&mut self, v: &str) {
        self.write_bytes(v.as_bytes()).await;
    }

    async fn write_string(&mut self, v: &str) {
        self.write_bytes(v.as_bytes()).await;
        self.write_byte(0).await;
    }

    async fn write_bytes(&mut self, v: &[u8]) {
        self.stream.write_all(v).await.unwrap();
    }

    async fn write_byte(&mut self, v: u8) {
        self.stream.write_u8(v).await.unwrap();
    }

    async fn write_short(&mut self, v: &i16) {
        self.write_bytes(&if self.little_endian {
            v.to_le_bytes()
        } else {
            v.to_be_bytes()
        }).await;
    }

    async fn write_int(&mut self, v: &i32) {
        self.write_bytes(&if self.little_endian {
            v.to_le_bytes()
        } else {
            v.to_be_bytes()
        }).await;
    }

    async fn write_long(&mut self, v: &i64) {
        self.write_bytes(&if self.little_endian {
            v.to_le_bytes()
        } else {
            v.to_be_bytes()
        }).await;
    }

    async fn write_float(&mut self, v: &f32) {
        self.write_bytes(&if self.little_endian {
            v.to_le_bytes()
        } else {
            v.to_be_bytes()
        }).await;
    }

    async fn write_double(&mut self, v: &f64) {
        self.write_bytes(&if self.little_endian {
            v.to_le_bytes()
        } else {
            v.to_be_bytes()
        }).await;
    }

    // 读取
    async fn prepare_bytes(&mut self, length: usize) ->  Vec<u8> {
        loop {
            if self.buffer.len() >= length {
                return self.buffer.copy_to_bytes(length).to_vec();
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await.unwrap() {
                if self.buffer.is_empty() {
                    return Vec::new();
                }
            }
        }
    }

    async fn prepare_bytes_end_with(&mut self, terminator: u8) -> Vec<u8> {
        loop {
            let mut index = 0;
            let mut has = false;
            for byte in self.buffer.chunk() {
                if byte.eq(&terminator) {
                    has = true;
                    break;
                }
                index += 1;
            }

            if has {
                let mut data = vec![0u8; index];
                self.buffer.copy_to_slice(&mut data);
                
                self.buffer.advance(1);
                return data;
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await.unwrap() {
                if self.buffer.is_empty() {
                    return Vec::new();
                }
            }
        }
    }

    async fn read_byte(&mut self) -> u8 {
        return self.prepare_bytes(1).await.remove(0);
    }

    async fn read_bytes(&mut self, size: usize) -> Vec<u8> {
        return self.prepare_bytes(size).await;
    }

    async fn read_short(&mut self) -> i16 {
        let dst = self.read_bytes(2).await.try_into().unwrap();

        return if self.little_endian {
            i16::from_le_bytes(dst)
        } else {
            i16::from_be_bytes(dst)
        }
    }

    async fn read_int(&mut self) -> i32 {
        let dst = self.read_bytes(4).await.try_into().unwrap();

        return if self.little_endian {
            i32::from_le_bytes(dst)
        } else {
            i32::from_be_bytes(dst)
        }
    }

    async fn read_long(&mut self) -> i64 {
        let dst = self.read_bytes(8).await.try_into().unwrap();

        return if self.little_endian {
            i64::from_le_bytes(dst)
        } else {
            i64::from_be_bytes(dst)
        }
    }

    async fn read_float(&mut self) -> f32 {
        let dst = self.read_bytes(4).await.try_into().unwrap();

        return if self.little_endian {
            f32::from_le_bytes(dst)
        } else {
            f32::from_be_bytes(dst)
        }
    }

    async fn read_double(&mut self) -> f64 {
        let dst = self.read_bytes(8).await.try_into().unwrap();

        return if self.little_endian {
            f64::from_le_bytes(dst)
        } else {
            f64::from_be_bytes(dst)
        }
    }

    async fn read_line(&mut self) -> String {
        // '\n'
        return self.read_utf(10).await;
    }

    async fn read_string(&mut self) -> String {
        // hex: 00

        return self.read_utf(0).await;
    }

    async fn read_utf(&mut self, terminator: u8) -> String {
        return unsafe {
            String::from_utf8_unchecked(self.prepare_bytes_end_with(terminator).await)
        };
    }
}

#[derive(Debug, Clone)]
pub enum DataValue {
    Void(),
    Bool(bool),
    Char(char),
    Short(i16),
    Int(i32),
    Long(i64),
    Date(Date),
    Month(),
    Time(Time),
    Minute(Minute),
    Second(),
    DateTime(DateTime),
    TimeStamp(),
    NanoTime(),
    NanoTimeStamp(),
    Float(f32),
    Double(f64),
    String(String),
    Symbol(String),

    Table(Table),
    Vector(Vector),
}

struct BasicValue {}
impl BasicValue {
    async fn decode(client: &mut TcpClient, num_type: i16) -> DataValue {
        return match num_type {
            DT_BOOL => {
                DataValue::Bool(BasicBool::decode(client).await)
            },
            DT_CHAR => {
                DataValue::Char(BasicChar::decode(client).await)
            },
            DT_SHORT => {
                DataValue::Short(BasicShort::decode(client).await)
            },
            DT_INT => {
                DataValue::Int(BasicInt::decode(client).await)
            },
            DT_LONG => {
                DataValue::Long(BasicLong::decode(client).await)
            },
            DT_DATE => {
                DataValue::Date(BasicDate::decode(client).await)
            },
            //Month
            DT_TIME => {
                DataValue::Time(BasicTime::decode(client).await)
            },
            DT_MINUTE => {
                DataValue::Minute(BasicMinute::decode(client).await)
            }
            //Second
            DT_DATETIME => {
                DataValue::DateTime(BasicDateTime::decode(client).await)
            },
            //TimeStamp
            //NanoTime
            //NanoTimeStamp
            DT_FLOAT => {
                DataValue::Float(BasicFloat::decode(client).await)
            },
            DT_DOUBLE => {
                DataValue::Double(BasicDouble::decode(client).await)
            },
            DT_STRING | DT_SYMBOL => {
                DataValue::String(BasicString::decode(client).await)
            },
            DT_VOID => {
                BasicBool::decode(client).await;
                DataValue::Void()
            },
            _ => {
                println!("ERROR");
                DataValue::Void()
            }
        }
    }

    async fn encode_no_type(client: &mut TcpClient, data_value: &DataValue) {
        match data_value {
            DataValue::Bool(v) => {
                BasicBool::encode(client, &v).await;
            },
            DataValue::Char(v) => {
                BasicChar::encode(client, &v).await;
            },
            DataValue::Short(v) => {
                BasicShort::encode(client, &v).await;
            },
            DataValue::Int(v) => {
                BasicInt::encode(client, &v).await;
            },
            DataValue::Long(v) => {
                BasicLong::encode(client, &v).await;
            },
            DataValue::Date(v) => {
                BasicDate::encode(client, &v).await;
            },
            //Month
            DataValue::Time(v) => {
                BasicTime::encode(client, &v).await;
            },
            //Minute
            //Second
            DataValue::DateTime(v) => {
                BasicDateTime::encode(client, &v).await;
            },
            //TimeStamp
            //NanoTime
            //NanoTimeStamp
            DataValue::Float(v) => {
                BasicFloat::encode(client, &v).await;
            },
            DataValue::Double(v) => {
                BasicDouble::encode(client, &v).await;
            },
            DataValue::String(v) | DataValue::Symbol(v) => {
                BasicString::encode(client, v).await;
            },
            _ => {
                println!("无匹配类型.")
            }
        }
    }

    async fn encode(client: &mut TcpClient, data_value: &DataValue) {
        match data_value {
            DataValue::Bool(v) => {
                let flag = (DF_SCALAR << 8) + DT_BOOL;
                BasicShort::encode(client, &flag).await;

                BasicBool::encode(client, &v).await;
            },
            DataValue::Char(v) => {
                let flag = (DF_SCALAR << 8) + DT_CHAR;
                BasicShort::encode(client, &flag).await;

                BasicChar::encode(client, &v).await;
            },
            DataValue::Short(v) => {
                let flag = (DF_SCALAR << 8) + DT_SHORT;
                BasicShort::encode(client, &flag).await;

                BasicShort::encode(client, &v).await;
            },
            DataValue::Int(v) => {
                let flag = (DF_SCALAR << 8) + DT_INT;
                BasicShort::encode(client, &flag).await;

                BasicInt::encode(client, &v).await;
            },
            DataValue::Long(v) => {
                let flag = (DF_SCALAR << 8) + DT_LONG;
                BasicShort::encode(client, &flag).await;

                BasicLong::encode(client, &v).await;
            },
            DataValue::Date(v) => {
                let flag = (DF_SCALAR << 8) + DT_INT;
                BasicShort::encode(client, &flag).await;

                BasicDate::encode(client, &v).await;
            },
            //Month
            DataValue::Time(v) => {
                let flag = (DF_SCALAR << 8) + DT_INT;
                BasicShort::encode(client, &flag).await;

                BasicTime::encode(client, &v).await;
            },
            //Minute
            //Second
            DataValue::DateTime(v) => {
                let flag = (DF_SCALAR << 8) + DT_INT;
                BasicShort::encode(client, &flag).await;

                BasicDateTime::encode(client, &v).await;
            },
            //TimeStamp
            //NanoTime
            //NanoTimeStamp
            DataValue::Float(v) => {
                let flag = (DF_SCALAR << 8) + DT_FLOAT;
                BasicShort::encode(client, &flag).await;

                BasicFloat::encode(client, &v).await;
            },
            DataValue::Double(v) => {
                let flag = (DF_SCALAR << 8) + DT_DOUBLE;
                BasicShort::encode(client, &flag).await;

                BasicDouble::encode(client, &v).await;
            },
            DataValue::String(v) | DataValue::Symbol(v) => {
                let flag = (DF_SCALAR << 8) + DT_STRING;
                BasicShort::encode(client, &flag).await;

                BasicString::encode(client, v).await;
            },
            DataValue::Table(v) => {
                BasicTable::encode(client, v).await;
            },
            DataValue::Vector(v) => {
                BasicVector::encode(client, v).await;
            },
            _ => {
                println!("无匹配类型.")
            }
        }
    }
}

struct BasicBool {}
impl BasicBool {
    async fn decode(client: &mut TcpClient) -> bool {
        return if client.read_byte().await == 0 {
            false
        } else {
            true
        };
    }
    async fn encode(client: &mut TcpClient, v: &bool) {
        client.write_byte(if *v {
            1
        } else {
            0
        }).await;
    }
}

struct BasicChar {}
impl BasicChar {
    async fn decode(client: &mut TcpClient) -> char {
        return client.read_byte().await as char;
    }
    async fn encode(client: &mut TcpClient, v: &char) {
        client.write_byte(*v as u8).await;
    }
}

struct BasicShort {}
impl BasicShort {
    async fn decode(client: &mut TcpClient) -> i16 {
        return client.read_short().await;
    }
    async fn encode(client: &mut TcpClient, v: &i16) {
        client.write_short(v).await;
    }
}

struct BasicInt {}
impl BasicInt {
    async fn decode(client: &mut TcpClient) -> i32 {
        return client.read_int().await;
    }
    async fn encode(client: &mut TcpClient, v: &i32) {
        client.write_int(v).await;
    }
}

struct BasicLong {}
impl BasicLong {
    async fn decode(client: &mut TcpClient) -> i64 {
        return client.read_long().await;
    }
    async fn encode(client: &mut TcpClient, v: &i64) {
        client.write_long(v).await;
    }
}

struct BasicDate {}
impl BasicDate {
    async fn decode(client: &mut TcpClient) -> Date {
        let days = BasicInt::decode(client).await;
        return BasicDate::int_to_date(days);
    }

    fn int_to_date(mut days: i32) -> Date {
        let days_c = days.clone();

        days += 719529;
        let circle_in400years = days / 146097;
        let offset_in400years = days % 146097;
        let result_year = circle_in400years * 400;
        let mut similar_years = offset_in400years / 365;
        let mut tmp_days = similar_years * 365;
        if similar_years > 0 {
            tmp_days += (similar_years - 1) / 4 + 1 - (similar_years - 1) / 100;
        }
        if tmp_days >= offset_in400years {
            similar_years -= 1;
        }
        let year = similar_years + result_year;
        days -= circle_in400years * 146097 + tmp_days;
        let leap = (year % 4 ==0 && year % 100 != 0) || year % 400 == 0;
        if days <= 0 {
            days += if leap {
                366
            } else {
                365
            };
        }

        let mut month;
        let day;
        if leap {
            month = days / 32 + 1;
            if days> CUM_LEAP_MONTH_DAYS[month as usize] {
                month += 1;
            }
            day = days - CUM_LEAP_MONTH_DAYS[month as usize - 1];
        } else {
            month = days / 32 + 1;
            if days > CUM_MONTH_DAYS[month as usize] {
                month += 1;
            }
            day = days - CUM_MONTH_DAYS[month as usize - 1];
        }

        return Date{
            raw: days_c,
            year,
            month,
            day
        };
    }

    async fn encode(client: &mut TcpClient, v: &Date) {
        BasicInt::encode(client, &BasicDate::date_to_int(&v)).await;
    }

    fn date_to_int(v: &Date) -> i32 {
        let year = v.year;
        let month = v.month;
        let day = v.day;

        if month < 1 || month > 12 || day < 0 {
            return -1;
        }

        let divide400years = year / 400;
        let offset400years = year % 400;
        let mut days = divide400years * 146097 + offset400years * 365 - 719529;
        if offset400years > 0 {
            days += (offset400years - 1) / 4 + 1 - (offset400years - 1) / 100;
        }

        if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
            days += CUM_LEAP_MONTH_DAYS[month as usize - 1];
            if day <= LEAP_MONTH_DAYS[month as usize - 1] {
                return days + day
            }
        } else {
            days += CUM_MONTH_DAYS[month as usize - 1];
            if day <= MONTH_DAYS[month as usize - 1] {
                return days + day
            }
        }

        return -2;
    }
}

//Month

struct BasicTime {}
impl BasicTime {
    async fn decode(client: &mut TcpClient) -> Time {
        let milliseconds = BasicInt::decode(client).await;
        return Time{
            raw: milliseconds,
            hour: milliseconds/3600000,
            minute: milliseconds/60000 % 60,
            second: milliseconds/1000 % 60,
            millisecond: milliseconds % 1000
        }
    }
    async fn encode(client: &mut TcpClient, v: &Time) {
        BasicInt::encode(client, &(((v.hour * 60 + v.minute) * 60 + v.second) * 1000 + v.millisecond)).await;
    }
}

struct BasicMinute;
impl BasicMinute {
    async fn decode(client: &mut TcpClient) -> Minute {
        let seconds = BasicInt::decode(client).await;
        return Minute{
            raw: seconds,
            hour: seconds / 60,
            minute: seconds % 60,
        }
    }
    async fn encode(client: &mut TcpClient, v: &Minute) {
        BasicInt::encode(client, &(v.hour * 60 + v.minute)).await;
    }
}

//Minute

//Second

struct BasicDateTime {}
impl BasicDateTime {
    fn divide(x: i32, y: i32) -> i32 {
        let tmp=x / y;
        return if x >= 0 {
            tmp
        } else if x % y < 0 {
            tmp - 1
        } else {
            tmp
        }
    }

    async fn decode(client: &mut TcpClient) -> DateTime {
        let mut seconds = client.read_int().await;
        seconds = BasicDateTime::divide(seconds, 86400);

        let date = BasicDate::int_to_date(seconds);
        seconds = seconds % 86400;
        if seconds < 0 {
            seconds += 86400;
        }
        let hour = seconds/3600;
        seconds = seconds % 3600;
        let minute = seconds / 60;
        let second = seconds % 60;

        return DateTime{
            year: date.year,
            month: date.month,
            day: date.day,
            hour,
            minute,
            second,
        }
    }
    async fn encode(client: &mut TcpClient, v: &DateTime) {
        let days = BasicDate::date_to_int(&v.to_date());
        BasicInt::encode(client, &(days * 86400 + (v.hour * 60 + v.minute) * 60 + v.second)).await;
    }
}

struct BasicFloat {}
impl BasicFloat {
    async fn decode(client: &mut TcpClient) -> f32 {
        return client.read_float().await;
    }
    async fn encode(client: &mut TcpClient, v: &f32) {
        client.write_float(v).await;
    }
}

struct BasicDouble {}
impl BasicDouble {
    async fn decode(client: &mut TcpClient) -> f64 {
        return client.read_double().await;
    }
    async fn encode(client: &mut TcpClient, v: &f64) {
        client.write_double(v).await;
    }
}

struct BasicString {}
impl BasicString {
    async fn decode(client: &mut TcpClient) -> String {
        return client.read_string().await;
    }
    async fn encode(client: &mut TcpClient, v: &str) {
        client.write_string(v).await;
    }
}

//
struct BasicTable {}
impl BasicTable {
    async fn decode(client: &mut TcpClient) -> DataValue {
        let _rows = client.read_int().await;
        let cols = client.read_int().await;
        let table_name = client.read_string().await;

        let mut names: Vec<String> = Vec::with_capacity(cols as usize);
        for _ in 0..cols {
            let name = client.read_string().await;
            names.push(name);
        }

        let mut colvs = Vec::with_capacity(cols as usize);
        for _ in 0..cols {
            let name = names.remove(0);

            let flag = client.read_short().await;
            let num_form = flag >> 8;
            let num_type = flag & 0xff;

            //debug!("T:F: {} T:T{}", num_form, num_type);

            if num_form != DF_VECTOR {
                //error!("Invalid form for column [{}] for table {}", name.clone(), table_name);
                return DataValue::Void();
            }

            let v = match BasicVector::decode(client, num_type).await {
                DataValue::Vector(v) => {
                    v
                },
                _ => {
                    Vector::new()
                }
            };

            colvs.push(TableColumn{
                name,
                num_type,
                data: v,
            })
        }

        return DataValue::Table(Table{
            name: table_name,
            cols: colvs,
        });
    }
    async fn encode(client: &mut TcpClient, v: &Table) {
        let flag = (DF_TABLE << 8) + DT_DICTIONARY;
        BasicShort::encode(client, &flag).await;
        BasicInt::encode(client, &v.row_len()).await;
        BasicInt::encode(client, &v.col_len()).await;
        BasicString::encode(client, "").await;
        for c in v.get_col() {
            BasicString::encode(client, &c.name).await;
        }
        for c in v.get_col() {
            BasicVector::encode(client, &c.data).await;
        }
    }
}

struct BasicVector {}
impl BasicVector {
    async fn decode(client: &mut TcpClient, num_type: i16) -> DataValue {
        let rows = client.read_int().await;
        let cols = client.read_int().await;
        let size = rows * cols;

        let mut rows = Vec::with_capacity(size as usize);
        for _ in 0..size {
            rows.push(BasicValue::decode(client, num_type).await)
        }

        return DataValue::Vector(Vector{
            num_type,
            data: rows,
        });
    }
    async fn encode(client: &mut TcpClient, v: &Vector) {
        let num_flag = (DF_VECTOR << 8) + v.get_num_type();
        BasicShort::encode(client, &num_flag).await;
        BasicInt::encode(client, &v.len()).await;
        BasicInt::encode(client, &1).await;

        for dv in v.get() {
            BasicValue::encode_no_type(client, dv).await;
        }
    }
}
