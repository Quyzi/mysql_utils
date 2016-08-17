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

#[macro_use]
extern crate log;
extern crate simplelog;

use simplelog::SimpleLogger;
use simplelog::LogLevelFilter;

use std::io::{BufReader, BufWriter};
use std::io;
use std::io::ErrorKind;
use std::io::prelude::*;

#[test]
fn test_transform() {
    // 0x5c30 => 0x00
    let test1 = &[0x5c, 0x30];
    let mut input = std::io::Cursor::new(test1);
    let mut output: Vec<u8> = Vec::new();
    process_input(&mut input, &mut output);
    println!("output: {:?}", output);
    assert_eq!(output, vec![0x00]);

    // TODO: Broken
    // 0x5c5c30 => 0x5c5c30 //Broken
    let test2 = &[0x5c, 0x5c, 0x30];
    let mut input2 = std::io::Cursor::new(test2);
    let mut output2: Vec<u8> = Vec::new();
    process_input(&mut input2, &mut output2);
    println!("Output2: {:?}", output2);
    assert_eq!(output2, vec![0x5c, 0x5c, 0x30]);

    // 0x5c5c5c30 => 0x5c5c00
    let test3 = &[0x5c, 0x5c, 0x5c, 0x30];
    let mut input3 = std::io::Cursor::new(test3);
    let mut output3: Vec<u8> = Vec::new();
    process_input(&mut input3, &mut output3);
    println!("output3: {:?}", output3);
    assert_eq!(output3, vec![0x5c, 0x5c, 0x00]);

    // 0x5c5c5c5c30 => 0x5c5c5c5c30
    let test4 = &[0x5c, 0x5c, 0x5c, 0x5c, 0x30];
    let mut input4 = std::io::Cursor::new(test4);
    let mut output4: Vec<u8> = Vec::new();
    process_input(&mut input4, &mut output4);
    println!("output4: {:?}", output4);
    assert_eq!(output4, vec![0x5c, 0x5c, 0x5c, 0x5c, 0x30]);

    // 0x5c5c5c3030 => 0x5c5c0030
    let test5 = &[0x5c, 0x5c, 0x5c, 0x30, 0x30];
    let mut input5 = std::io::Cursor::new(test5);
    let mut output5: Vec<u8> = Vec::new();
    process_input(&mut input5, &mut output5);
    println!("output5: {:?}", output5);
    assert_eq!(output5, vec![0x5c, 0x5c, 0x00, 0x30]);

}

fn write_byte<W>(byte: u8, writer: &mut W) -> Result<usize, std::io::Error>
    where W: Write
{
    let written_bytes = try!(writer.write(&[byte]));
    if written_bytes < 1 {
        error!("Unable to write byte: {}", byte);
        Err(std::io::Error::new(ErrorKind::WriteZero, "Could not write byte"))
    } else {
        Ok(written_bytes)
    }
}

fn process_input<R, W>(mut reader: R, mut writer: W) -> Result<(), std::io::Error>
    where R: BufRead,
          W: Write
{
    // As long as there's another byte this loop will continue
    'outer: loop {
        let next_byte = reader.by_ref().bytes().next();

        match next_byte {
            Some(read_byte) => {
                let read_byte = try!(read_byte);

                // Fast forward through bytes that don't match 0x5c
                if read_byte != 0x5c {
                    try!(write_byte(read_byte, &mut writer));
                    continue;
                }

                let mut count: u64 = 0;
                for byte in reader.by_ref().bytes() {
                    let read_byte = byte.unwrap();
                    if read_byte == 0x30 {
                        if count % 2 == 0 {
                            // we saw 0 or even number of 0x5c before 0x5c30
                            try!(write_byte(0x00, &mut writer));
                            break;
                        } else {
                            // we saw odd number of 0x5c before 0x5c30. put the outstanding 0c5c
                            // in the output,
                            // and then 0x30
                            //
                            try!(write_byte(0x5c, &mut writer));
                            try!(write_byte(0x30, &mut writer));
                            break;
                        }
                    } else if read_byte == 0x5c {
                        try!(write_byte(0x5c, &mut writer));
                        count += 1;
                    } else {
                        // put the outstanding 0x5c and the char we just read in output
                        try!(write_byte(0x5c, &mut writer));
                        try!(write_byte(read_byte, &mut writer));
                        break;
                    }
                }
            }
            None => {
                break 'outer;
            }
        }
    }
    // EOF
    // writer will flush when dropped
    Ok(())
}

fn main() {
    let _ = SimpleLogger::init(LogLevelFilter::Trace);
    // Implicit synchronization
    let mut stdin = BufReader::with_capacity(128 * 1024, io::stdin());
    // BufWriter with 128K capacity.  Try to make our writes large for efficient
    // downstream consumption
    let mut writer = BufWriter::with_capacity(128 * 1024, io::stdout());

    match process_input(&mut stdin, &mut writer) {
        Ok(_) => {}
        Err(e) => {
            error!("Failed with error: {}", e);
        }
    };
}
