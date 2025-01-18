use std::io::Cursor;
use byteorder::{BigEndian, LittleEndian};
use byteorder::{ReadBytesExt, WriteBytesExt};


fn write_numbers_to_file() -> (u32, i8, f64) {
    let mut w = vec![];
    let one: u32 = 1;
    let two: i8 = 2;
    let three: f64 = 3.14;

    w.write_u32::<LittleEndian>(one).unwrap();
    println!("{:?}", &w);

    w.write_i8(two).unwrap();
    println!("{:?}", &w);

    w.write_f64::<LittleEndian>(three).unwrap();
    println!("{:?}", &w);

    (one, two, three)
}



fn read_numbers_from_file() -> (u32, i8, f64) {
    let mut r = Cursor::new(vec![1, 0, 0, 0, 2, 31, 133, 235, 81, 184, 30, 9, 64]);
    let one: u32 = r.read_u32::<LittleEndian>().unwrap();
    let two: i8 = r.read_i8().unwrap();
    let three: f64 = r.read_f64::<LittleEndian>().unwrap();

    (one, two, three)
}

fn main() {
    let (one, two, three) = write_numbers_to_file();
    let (one_, two_, three_) = read_numbers_from_file();

    println!("{}={}", three, three_);
}

