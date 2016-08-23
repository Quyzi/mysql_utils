//!    This program has been created to handle an incompatibility between how mysqldump escapes
//!    some characters and how hive interprets those escaped chars. It does the following:
//!    If you see an 0x5c30 in the input sequence
//!    a. and there is no or even number of 0x5c before 0x5c30, translate this 0x5c30 to 0x00
//!    b. if there is odd number of 0x5c before 0x5c30, don't do anything.
//!    Some sample transforms:
//!      0x5c30 => 0x00
//!      0x5c5c30 => 0x5c5c30
//!      0x5c5c5c30 => 0x5c5c00
//!      0x5c5c5c5c30 => 0x5c5c5c5c30
//!      0x5c5c5c3030 => 0x5c5c0030
//!      0x5c5c5c5c3030 => 0x5c5c5c5c3030
//!      0x5c5c5c40 => 0x5c5c5c40
//!      0x5c5c5c5c40 => 0x5c5c5c5c40
//!    Here is another way to test:
//!    - Create table with blob content:  create table MyTest (id integer, value1 varchar(20),
//!      content blob, value2 double, primary key(id));
//!    - Insert into blob content:  insert into MyTest (id, value1, content, value2)
//!      values (1, "data1", 0x3020090d0a2227005c30, 2.2);
//!    - checking content: select hex(content) from MyTest;
//!    - chmod a+rw /tmp/dump
//!    - mysqldump -u root --tab=/tmp/dump --single-transaction -- create-options test
//!    - see content:  hexdump /tmp/dump/MyTest.txt
//!    hexdump of original dump file:
//!    0000000 31 09 64 61 74 61 31 09 30 20 5c 09 0d 5c 0a 22
//!    0000010 27 5c 30 5c 5c 30 09 32 2e 32 0a
//!    000001b
//!    hexdump after passing through this program:
//!    0000000 31 09 64 61 74 61 31 09 30 20 5c 09 0d 5c 0a 22
//!    0000010 27 00 5c 5c 30 09 32 2e 32 0a
//!    000001a
//!    Author : vamsi Nov 2015

use std::io::{BufReader, BufWriter};
use std::io;
use std::io::ErrorKind;
use std::io::prelude::*;


#[test]
fn test_transform_1() {
    // 0x5c30 => 0x00
    let test1 = &[0x5c, 0x30];
    let mut input = std::io::Cursor::new(test1);
    let mut output: Vec<u8> = Vec::new();
    process_input(&mut input, &mut output);
    println!("output1: {:?}", output);
    assert_eq!(output, vec![0x00]);
}

#[test]
fn test_transform_2() {
    // 0x5c5c30 => 0x5c5c30
    let test2 = &[0x5c, 0x5c, 0x30];
    let mut input2 = std::io::Cursor::new(test2);
    let mut output2: Vec<u8> = Vec::new();
    process_input(&mut input2, &mut output2);
    println!("output2: {:?}", output2);
    assert_eq!(output2, vec![0x5c, 0x5c, 0x30]);
}

#[test]
fn test_transform_3() {
    // 0x5c5c5c30 => 0x5c5c00
    let test3 = &[0x5c, 0x5c, 0x5c, 0x30];
    let mut input3 = std::io::Cursor::new(test3);
    let mut output3: Vec<u8> = Vec::new();
    process_input(&mut input3, &mut output3);
    println!("output3: {:?}", output3);
    assert_eq!(output3, vec![0x5c, 0x5c, 0x00]);
}

#[test]
fn test_transform_4() {
    // 0x5c5c5c5c30 => 0x5c5c5c5c30
    let test4 = &[0x5c, 0x5c, 0x5c, 0x5c, 0x30];
    let mut input4 = std::io::Cursor::new(test4);
    let mut output4: Vec<u8> = Vec::new();
    process_input(&mut input4, &mut output4);
    println!("output4: {:?}", output4);
    assert_eq!(output4, vec![0x5c, 0x5c, 0x5c, 0x5c, 0x30]);
}

#[test]
fn test_transform_5() {
    // 0x5c5c5c3030 => 0x5c5c0030
    let test5 = &[0x5c, 0x5c, 0x5c, 0x30, 0x30];
    let mut input5 = std::io::Cursor::new(test5);
    let mut output5: Vec<u8> = Vec::new();
    process_input(&mut input5, &mut output5);
    println!("output5: {:?}", output5);
    assert_eq!(output5, vec![0x5c, 0x5c, 0x00, 0x30]);
}

#[test]
fn test_transform_6() {
    // 0x5c5c5c5c3030 => 0x5c5c5c5c3030
    let test6 = &[0x5c, 0x5c, 0x5c, 0x5c, 0x30, 0x30];
    let mut input6 = std::io::Cursor::new(test6);
    let mut output6: Vec<u8> = Vec::new();
    process_input(&mut input6, &mut output6);
    println!("output6: {:?}", output6);
    assert_eq!(output6, vec![0x5c, 0x5c, 0x5c, 0x5c, 0x30, 0x30]);
}

#[test]
fn test_transform_7() {
    // 0x5c5c5c40 => 0x5c5c5c40
    let test7 = &[0x5c, 0x5c, 0x5c, 0x40];
    let mut input7 = std::io::Cursor::new(test7);
    let mut output7: Vec<u8> = Vec::new();
    process_input(&mut input7, &mut output7);
    println!("output7: {:?}", output7);
    assert_eq!(output7, vec![0x5c, 0x5c, 0x5c, 0x40]);
}

#[test]
fn test_transform_8() {
    // 0x5c5c5c5c40 => 0x5c5c5c5c40
    let test8 = &[0x5c, 0x5c, 0x5c, 0x5c, 0x40];
    let mut input8 = std::io::Cursor::new(test8);
    let mut output8: Vec<u8> = Vec::new();
    process_input(&mut input8, &mut output8);
    println!("output8: {:?}", output8);
    assert_eq!(output8, vec![0x5c, 0x5c, 0x5c, 0x5c, 0x40]);
}

#[test]
fn test_transform_9() {
    // 0x5c5c5c5c40 => 0x5c5c5c5c40
    let test8 = &[0x5c, 0x24, 0x5c, 0x5c, 0x40];
    let mut input8 = std::io::Cursor::new(test8);
    let mut output8: Vec<u8> = Vec::new();
    process_input(&mut input8, &mut output8);
    println!("output9: {:?}", output8);
    assert_eq!(output8, vec![0x5c, 0x24, 0x5c, 0x5c, 0x40]);
}

fn write_byte<W>(byte: u8, writer: &mut W) -> Result<usize, std::io::Error>
    where W: Write
{
    let written_bytes = try!(writer.write(&[byte]));
    if written_bytes < 1 {
        Err(std::io::Error::new(ErrorKind::WriteZero, "Could not write byte"))
    } else {
        Ok(written_bytes)
    }
}

fn process_byte<W: Write>(byte: u8, writer: &mut W, count: &mut u64) -> Result<(), std::io::Error> {
    // println!("Processing {} with count == {}", byte, count);
    if byte == 0x30 {
        if (*count + 1) % 2 == 0 {
            // we saw 0 or even number of 0x5c before 0x5c30
            try!(write_byte(0x00, writer));
            return Ok(());
        } else {
            // we saw odd number of 0x5c before 0x5c30. put the outstanding 0c5c
            // in the output,
            // and then 0x30
            //
            try!(write_byte(0x5c, writer));
            try!(write_byte(0x30, writer));
            return Ok(());
        }
    } else if byte == 0x5c {
        try!(write_byte(0x5c, writer));
        *count += 1;
    } else {
        // put the outstanding 0x5c and the char we just read in output
        try!(write_byte(0x5c, writer));
        try!(write_byte(byte, writer));
        return Ok(());
    }
    Err(std::io::Error::new(std::io::ErrorKind::Other, ""))
}

fn process_input<R, W>(mut reader: R, mut writer: W) -> Result<(), std::io::Error>
    where R: BufRead,
          W: Write
{
    // As long as there's another byte this loop will continue
    loop {
        let mut buffer: Vec<u8> = Vec::with_capacity(1024 * 128);
        let read = try!(reader.read_until(0x5c, &mut buffer));
        if read == 0 {
            return Ok(());
        }
        if let Some(last) = buffer.pop() {
            try!(writer.write(&buffer[..buffer.len()]));
            if last != 0x5c {
                try!(writer.write(&[last]));
            }
            let mut count: u64 = 1;
            for byte in reader.by_ref().bytes() {
                if let Ok(read_byte) = byte {
                    if let Ok(_) = process_byte(read_byte, &mut writer, &mut count) {
                        break;
                    }
                }
            }
        }
    }
}

fn main() {
    // Implicit synchronization
    let mut stdin = BufReader::with_capacity(128 * 1024, io::stdin());
    // BufWriter with 128K capacity.  Try to make our writes large for efficient
    // downstream consumption
    let mut writer = BufWriter::with_capacity(128 * 1024, io::stdout());

    match process_input(&mut stdin, &mut writer) {
        Ok(_) => {}
        Err(e) => {
            println!("Failed with error: {}", e);
        }
    };
}
