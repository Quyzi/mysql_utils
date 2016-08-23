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
extern crate memchr;
extern crate simplelog;

use memchr::{memchr, memchr2};
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
    process_input2(&mut input, &mut output);
    println!("output1: {:?}", output);
    assert_eq!(output, vec![0x00]);

    // 0x5c5c30 => 0x5c5c30
    let test2 = &[0x5c, 0x5c, 0x30];
    let mut input2 = std::io::Cursor::new(test2);
    let mut output2: Vec<u8> = Vec::new();
    process_input2(&mut input2, &mut output2);
    println!("output2: {:?}", output2);
    assert_eq!(output2, vec![0x5c, 0x5c, 0x30]);

    // 0x5c5c5c30 => 0x5c5c00
    let test3 = &[0x5c, 0x5c, 0x5c, 0x30];
    let mut input3 = std::io::Cursor::new(test3);
    let mut output3: Vec<u8> = Vec::new();
    process_input2(&mut input3, &mut output3);
    println!("output3: {:?}", output3);
    assert_eq!(output3, vec![0x5c, 0x5c, 0x00]);

    // 0x5c5c5c5c30 => 0x5c5c5c5c30
    let test4 = &[0x5c, 0x5c, 0x5c, 0x5c, 0x30];
    let mut input4 = std::io::Cursor::new(test4);
    let mut output4: Vec<u8> = Vec::new();
    process_input2(&mut input4, &mut output4);
    println!("output4: {:?}", output4);
    assert_eq!(output4, vec![0x5c, 0x5c, 0x5c, 0x5c, 0x30]);

    // 0x5c5c5c3030 => 0x5c5c0030
    let test5 = &[0x5c, 0x5c, 0x5c, 0x30, 0x30];
    let mut input5 = std::io::Cursor::new(test5);
    let mut output5: Vec<u8> = Vec::new();
    process_input2(&mut input5, &mut output5);
    println!("output5: {:?}", output5);
    assert_eq!(output5, vec![0x5c, 0x5c, 0x00, 0x30]);

    // 0x5c5c5c5c3030 => 0x5c5c5c5c3030
    let test6 = &[0x5c, 0x5c, 0x5c, 0x5c, 0x30, 0x30];
    let mut input6 = std::io::Cursor::new(test6);
    let mut output6: Vec<u8> = Vec::new();
    process_input2(&mut input6, &mut output6);
    println!("output6: {:?}", output6);
    assert_eq!(output6, vec![0x5c, 0x5c, 0x5c, 0x5c, 0x30, 0x30]);

    // 0x5c5c5c40 => 0x5c5c5c40
    let test7 = &[0x5c, 0x5c, 0x5c, 0x40];
    let mut input7 = std::io::Cursor::new(test7);
    let mut output7: Vec<u8> = Vec::new();
    process_input2(&mut input7, &mut output7);
    println!("output7: {:?}", output7);
    assert_eq!(output7, vec![0x5c, 0x5c, 0x5c, 0x40]);

    // 0x5c5c5c5c40 => 0x5c5c5c5c40
    let test8 = &[0x5c, 0x5c, 0x5c, 0x5c, 0x40];
    let mut input8 = std::io::Cursor::new(test8);
    let mut output8: Vec<u8> = Vec::new();
    process_input2(&mut input8, &mut output8);
    println!("output8: {:?}", output8);
    assert_eq!(output8, vec![0x5c, 0x5c, 0x5c, 0x5c, 0x40]);
}

fn write_byte<W>(byte: u8, writer: &mut W) -> Result<usize, std::io::Error>
    where W: Write
{
    let written_bytes = try!(writer.write(&[byte]));
    Ok(written_bytes)
}

fn process_input2<R, W>(mut reader: R, mut writer: W) -> Result<(), std::io::Error>
    where R: BufRead,
          W: Write
{
    // a. and there is no or even number of 0x5c before 0x5c30, translate this 0x5c30 to 0x00
    // b. if there is odd number of 0x5c before 0x5c30, don't do anything.
    // Ok lets try this again
    let mut buffer: Vec<u8> = Vec::with_capacity(1024 * 128);
    loop {
        let read_size = try!(reader.read(&mut buffer[..]));
        let mut five_c: u64 = 0;

        // check for likely case first
        if read_size > 0 {
            let mut start_position: usize = 0;
            loop {
                println!("looping");
                // Search for either 0x5c or 0x30
                match memchr2(0x5c, 0x30, &buffer[start_position..]) {
                    Some(location) => {
                        // Move the start_position up to here
                        if buffer[location] == 0x5c {
                            println!("Found 0x5c");
                            // This just counts the 0x5c's
                            five_c += 1;
                            println!("five_c = {}", five_c);
                        } else if buffer[location] == 0x30 {
                            println!("Found 0x30");
                            // Make sure we can go back one byte
                            if location > 0 {
                                if buffer[location - 1] == 0x5c {
                                    // Now we have 0x5c, 0x30
                                    if (five_c - 1) % 2 == 0 {
                                        // 0x00 should be written
                                        // Remove the 0x30
                                        println!("buffer.remove({})", location);
                                        buffer.remove(location);
                                        // Change the 0x5c to 0x00
                                        println!("buffer[{}-1] = 0x00", location);
                                        buffer[location - 1] = 0x00;
                                    } else {
                                        // odd number of 0x5c's
                                        // no-op
                                    }
                                } else {
                                    // No 0x5c before this 0x30
                                    // no-op
                                }
                            } else {
                                // location == 0
                                // no-op
                            }
                        }
                        println!("start_position = {}", location);
                        // Advance the start position
                        start_position = location;
                    }
                    None => {
                        // write this entire buffer back out.
                        println!("Writing the entire buffer out");
                        try!(writer.write(&buffer[..]));
                        buffer.clear();
                        // break the inner loop
                        break;
                    }
                }
            }
        } else {
            // Break the outer loop
            println!("Break outer loop");
            break;
        }
    }
    Ok(())
}

fn process_input<R, W>(mut reader: R, mut writer: W) -> Result<(), std::io::Error>
    where R: BufRead,
          W: Write
{
    // As long as there's another byte this loop will continue
    let mut buffer = [0; 1024 * 128];

    'outer: loop {
        let read_size = try!(reader.read(&mut buffer[..]));
        println!("Read_size: {}", read_size);
        if read_size > 0 {
            let mut count: u64 = 0;
            let mut buf_position: usize = 0;

            // This inner loop is so we fully read the buffer before trying to read again
            'inner: loop {
                if buf_position >= read_size {
                    break 'inner;
                }

                // Fast forward through anything that isn't 0x5c
                match memchr(0x5c, &buffer[buf_position..]) {
                    Some(location) => {
                        // We found a 0x5c at location
                        println!("Found 0x5c at: {}", location);
                        try!(writer.write(&buffer[buf_position..location]));

                        buf_position = location + 1;
                        // Now handle the cases
                        println!("for _ in {}..{}", buf_position, read_size);
                        for _ in buf_position..read_size {
                            if buffer[buf_position] == 0x30 {
                                if count % 2 == 0 {
                                    // we saw 0 or even number of 0x5c before 0x5c30
                                    println!("write_byte 0x00");
                                    try!(write_byte(0x00, &mut writer));
                                    buf_position += 1;
                                    break;
                                } else {
                                    // we saw odd number of 0x5c before 0x5c30.
                                    // put the outstanding 0c5c
                                    // in the output,
                                    // and then 0x30
                                    //
                                    println!("write_byte 0x5c + 0x30");
                                    try!(write_byte(0x5c, &mut writer));
                                    try!(write_byte(0x30, &mut writer));
                                    buf_position += 1;
                                    break;
                                }
                            } else if buffer[buf_position] == 0x5c {
                                println!("write_byte 0x5c");
                                try!(write_byte(0x5c, &mut writer));
                                buf_position += 1;
                                count += 1;
                            } else {
                                // put the outstanding 0x5c and the char we just read in output
                                println!("write_byte 0x5c + outstanding");
                                try!(write_byte(0x5c, &mut writer));
                                try!(write_byte(buffer[buf_position], &mut writer));
                                buf_position += 1;
                                // break;
                            }
                        }
                    }
                    None => {
                        try!(writer.write(&buffer[buf_position..read_size]));
                        // We can skip right to the next buffer read
                        println!("Break 'inner");
                        break 'inner;
                    }
                };
            }
        } else {
            println!("Done = true");
            break 'outer;
        }
    }
    // EOF
    // writer will flush when dropped
    Ok(())
}

fn main() {
    let _ = SimpleLogger::init(LogLevelFilter::Trace);
    // let stdin = io::stdin();
    // let mut stdin = stdin.lock();
    let mut stdin = BufReader::with_capacity(256 * 1024, io::stdin());
    // BufWriter with 128K capacity.  Try to make our writes large for efficient
    // downstream consumption
    let mut writer = BufWriter::with_capacity(256 * 1024, io::stdout());

    match process_input(&mut stdin, &mut writer) {
        Ok(_) => {}
        Err(e) => {
            error!("Failed with error: {}", e);
        }
    };
}
