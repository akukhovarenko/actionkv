use std::{collections::HashMap, fs::{File, OpenOptions}, io::{self, BufRead, BufReader, BufWriter, ErrorKind, Seek, SeekFrom, Write} , path::Path};
use byteorder::LittleEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde_derive::{Serialize, Deserialize};

type ByteString = Vec<u8>;
type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug)]
pub struct ActionKV {
    f: File,
    pub index: HashMap<ByteString, u64>,
}

impl ActionKV {
    pub fn process_record<R: BufRead>(f: &mut R) -> io::Result<KeyValuePair> {
        let mut buffer = [0;2];
        let result = f.read(&mut buffer)?;
        if result == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        Ok(KeyValuePair {key: vec![buffer[0]], value: vec![buffer[1]]})
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = HashMap::new();
        Ok(ActionKV{f, index})
    }

    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.f);
        
        loop {
            let position = f.seek(SeekFrom::Current(0))?;
            let kv_pair = ActionKV::process_record(&mut f);
            let kv = match kv_pair {
                Ok(kv) => kv,
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => break,
                        _ => return Err(err)
                    }
                },
            };
            self.index.insert(kv.key, position);
        };
        Ok(())
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let mut f = BufWriter::new(&mut self.f);

        let key_len = key.len();
        let value_len = value.len();

        let mut tmp = ByteString::with_capacity(key_len + value_len);

        for byte in key {
            tmp.push(*byte);
        }

        for byte in value {
            tmp.push(*byte);
        }
         
        f.seek(SeekFrom::End(0))?;
        let position = f.seek(SeekFrom::Current(0))?;

        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(value_len as u32)?;
        f.write_all(&tmp)?;

        self.index.insert(key.to_vec(), position);

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempfile, NamedTempFile};
    use std::io::Write;

    fn init_file(data: &mut ByteString) -> File {
        let mut f = tempfile().unwrap();
        f.write(data.as_slice()).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f
    }

    fn get_test_action_kv() -> ActionKV {
        ActionKV {f: init_file(Vec::from(b"abcd").as_mut()), index: HashMap::new()}
    }

    #[test]
    fn actionkv_open(){
        let mut tempfile = NamedTempFile::new().unwrap();
        writeln!(tempfile, "abcd").unwrap();
        let store = ActionKV::open(tempfile.path());
        assert!(store.is_ok())
    }

    #[test]
    fn actionkv_insert(){
        let mut tempfile = NamedTempFile::new().unwrap();
        let mut  store = ActionKV::open(tempfile.path()).unwrap();

        let path = tempfile.path();
        let result = store.insert(&[0xAA, 0xAB, 0xAC], &[130, 140, 150]);
        let result = store.insert(&[150, 160, 170], &[180, 190, 200]);
        tempfile.flush();
    }

    #[test]
    fn actionkv_load_no_such_file(){
        let mut store = ActionKV::open(Path::new("foo_file")).unwrap();
        let result = store.load();
        assert!(result.is_ok());
        assert!(store.index == HashMap::new())
    }

    #[test]
    fn actionkv_process_record() {
        let f = init_file(Vec::from(b"abcd").as_mut());
        let mut buffer = BufReader::new(f);
        let kv_pair = ActionKV::process_record(&mut buffer);
        assert!(kv_pair.is_ok());
        assert_eq!(kv_pair.unwrap(), KeyValuePair {key: vec![b'a'], value: vec![b'b']});
        
        let kv_pair  = ActionKV::process_record(&mut buffer);
        assert!(kv_pair.is_ok());
        assert_eq!(kv_pair.unwrap(), KeyValuePair {key: vec![b'c'], value: vec![b'd']});

        let kv_pair = ActionKV::process_record(&mut buffer);
        assert!(kv_pair.is_err_and(|x| x.kind() == io::ErrorKind::UnexpectedEof));
    }

    #[test]
    fn actionkv_load() {
        let mut store = get_test_action_kv();
        let key: &ByteStr = b"c";
        let result = store.load();
        assert!(result.is_ok());
        let data = store.index.get(key).unwrap();
        assert_eq!(*data, 2);
    }
}