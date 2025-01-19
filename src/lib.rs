use std::{collections::HashMap, fs::{File, OpenOptions}, io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write} , path::Path};
use byteorder::LittleEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde_derive::{Serialize, Deserialize};

type ByteString = Vec<u8>;
type ByteStr = [u8];

#[derive(Debug)]
pub enum KVError {
    IndexError,
    PositionError,
    ProcessRecordError,
    FileError,
    WriteError,
}

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
    pub fn new(path: &Path) -> Result<ActionKV, KVError> {
        let mut store = ActionKV::open(path).or(Err(KVError::FileError))?;
        store.load()?;
        Ok(store)
    }
    
    fn open(path: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = HashMap::new();
        Ok(ActionKV{f, index})
    }

    pub fn process_record<R: BufRead>(f: &mut R) -> io::Result<KeyValuePair> {
        let key_len = f.read_u32::<LittleEndian>()?;
        let value_len = f.read_u32::<LittleEndian>()?;
        let mut key = ByteString::with_capacity(key_len as usize);
        f.take(key_len as u64).read_to_end(&mut key)?;
        let mut value = ByteString::with_capacity(value_len as usize);
        f.take(value_len as u64).read_to_end(&mut value)?;
        Ok(KeyValuePair {key, value})
    }

    fn load(&mut self) -> Result<(), KVError> {
        let mut f = BufReader::new(&mut self.f);
        loop {
            let position = f.seek(SeekFrom::Current(0)).or(Err(KVError::PositionError))?;
            let kv_pair = ActionKV::process_record(&mut f);
            let kv = match kv_pair {
                Ok(kv) => kv,
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => break,
                        _ => return Err(KVError::ProcessRecordError)
                    }
                },
            };
            self.index.insert(kv.key, position);
        };
        Ok(())
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> Result<(), KVError> {
        let mut f = BufWriter::new(&mut self.f);
        let key_len = key.len();
        let value_len = value.len();
        let mut key_value = ByteString::with_capacity(key_len + value_len);

        for byte in key {
            key_value.push(*byte);
        }
        for byte in value {
            key_value.push(*byte);
        }
         
        f.seek(SeekFrom::End(0)).or(Err(KVError::PositionError))?;
        let position = f.seek(SeekFrom::Current(0)).or(Err(KVError::PositionError))?;

        f.write_u32::<LittleEndian>(key_len as u32).or(Err(KVError::WriteError))?;
        f.write_u32::<LittleEndian>(value_len as u32).or(Err(KVError::WriteError))?;
        f.write_all(&key_value).or(Err(KVError::WriteError))?;
        f.flush().or(Err(KVError::WriteError))?;

        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    pub fn get(&mut self, key: &ByteStr) -> Result<ByteString, KVError> {
        let position = self.index.get(key).ok_or(KVError::IndexError)?;
        self.f.seek(SeekFrom::Start(*position)).or(Err(KVError::PositionError))?;
        let mut f = BufReader::new(&mut self.f);
        let value = ActionKV::process_record(&mut f).or(Err(KVError::ProcessRecordError))?.value;

        Ok(value)
    }
    
    pub fn delete(&mut self, key: &ByteStr) -> Result<(), KVError> {
        self.insert(key, b"")
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempfile, NamedTempFile};
    use std::io::Write;

    fn init_file(data: &mut ByteString) -> NamedTempFile {
        let f = NamedTempFile::new().unwrap();
        let mut file = f.as_file();
        file.write(data.as_slice()).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        f
    }

    fn get_test_action_kv() -> ActionKV {
        let mut data: ByteString = vec![1, 0, 0, 0, 1, 0, 0, 0, 0xAA, 0xBB, 2, 0, 0, 0, 3, 0, 0, 0, 0xCC, 0xCD, 0xDD, 0xEE, 0xFF];
        ActionKV::new(init_file(&mut data).path()).unwrap()
    }

    #[test]
    fn actionkv_delete() {
        let mut store = get_test_action_kv();
        store.load().unwrap();
        let result = store.delete(&[0xAA]);
        assert!(result.is_ok());

    }

    #[test]
    fn actionkv_get() {
        let mut store = get_test_action_kv();
        store.load().unwrap();
        let result = store.get(&[0xAA]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), &[0xBB]); 
        let result = store.get(&[0xCC, 0xCD]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), &[0xDD, 0xEE, 0xFF]); 
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
        store.insert(&[0xAA], &[130, 140, 150]).unwrap();
        store.insert(&[0xBB], &[180, 190, 200]).unwrap();

        assert_eq!(store.index.get(&[0xAA].to_vec()).unwrap(), &0u64);
        assert_eq!(store.index.get(&[0xBB].to_vec()).unwrap(), &12u64);

        tempfile.flush().unwrap();
        assert_eq!(tempfile.as_file().metadata().unwrap().len(), 24);
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
        let f = init_file(vec![1, 0, 0, 0, 1, 0, 0, 0, 0xAA, 0xBB, 1, 0, 0, 0, 2, 0, 0, 0, 0xCC, 0xDD, 0xEE].as_mut());
        let mut buffer = BufReader::new(f);
        let kv_pair = ActionKV::process_record(&mut buffer);
        assert!(kv_pair.is_ok());
        assert_eq!(kv_pair.unwrap(), KeyValuePair {key: vec![0xAA], value: vec![0xBB]});
        
        let kv_pair  = ActionKV::process_record(&mut buffer);
        assert!(kv_pair.is_ok());
        assert_eq!(kv_pair.unwrap(), KeyValuePair {key: vec![0xCC], value: vec![0xDD, 0xEE]});

        let kv_pair = ActionKV::process_record(&mut buffer);
        assert!(kv_pair.is_err_and(|x| x.kind() == io::ErrorKind::UnexpectedEof));
    }

    #[test]
    fn actionkv_load() {
        let mut store = get_test_action_kv();
        let key: &ByteStr = &[0xCCu8, 0xCD];
        let result = store.load();
        assert!(result.is_ok());
        let data = store.index.get(key).unwrap();
        assert_eq!(*data, 10);
    }
}