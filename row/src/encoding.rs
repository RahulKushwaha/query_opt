use expr::types::{DataType, Value};

/// Tag byte: 0=Null, 1=Bool, 2=Int, 3=Float, 4=Str
pub fn encode_value(v: &Value, buf: &mut Vec<u8>) {
    match v {
        Value::Null => buf.push(0),
        Value::Bool(b) => {
            buf.push(1);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::Int(i) => {
            buf.push(2);
            buf.extend_from_slice(&i.to_be_bytes());
        }
        Value::Float(f) => {
            buf.push(3);
            buf.extend_from_slice(&f.to_be_bytes());
        }
        Value::Str(s) => {
            buf.push(4);
            buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

pub fn decode_value(bytes: &[u8], _dt: &DataType) -> Value {
    if bytes.is_empty() {
        return Value::Null;
    }
    match bytes[0] {
        0 => Value::Null,
        1 => Value::Bool(bytes[1] != 0),
        2 => Value::Int(i64::from_be_bytes(bytes[1..9].try_into().unwrap())),
        3 => Value::Float(f64::from_be_bytes(bytes[1..9].try_into().unwrap())),
        4 => {
            let len = u16::from_be_bytes([bytes[1], bytes[2]]) as usize;
            Value::Str(String::from_utf8(bytes[3..3 + len].to_vec()).unwrap())
        }
        _ => Value::Null,
    }
}

/// Decode a value and return how many bytes were consumed.
pub fn decode_value_with_len(bytes: &[u8], dt: &DataType) -> (Value, usize) {
    if bytes.is_empty() {
        return (Value::Null, 0);
    }
    match bytes[0] {
        0 => (Value::Null, 1),
        1 => (Value::Bool(bytes[1] != 0), 2),
        2 => (decode_value(bytes, dt), 9),
        3 => (decode_value(bytes, dt), 9),
        4 => {
            let len = u16::from_be_bytes([bytes[1], bytes[2]]) as usize;
            (decode_value(bytes, dt), 3 + len)
        }
        _ => (Value::Null, 1),
    }
}
