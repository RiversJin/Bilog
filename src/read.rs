use std::{vec, time::SystemTime, fs::File, io::{BufReader, Seek, BufRead, Read}};
use chrono::{TimeZone, Offset};
use anyhow::{Result, anyhow};
use once_cell::sync::Lazy;
use regex::Regex;
use scopeguard::defer;

const MAX_SAMPLEING_LINE: usize = 1000;
#[derive(Debug)]
pub struct TimeFormat {
    description: String,
    regex: Regex,
}

static TIME_ZONE_OFFSET_SECOND: Lazy<i64> = Lazy::new(||{
    chrono::Local::now().offset().fix().local_minus_utc() as i64
});

enum FindTimeStampResult{
    Found(SystemTime),
    NotFound,
    Error(anyhow::Error),
}

impl TimeFormat {
    pub fn new(description: &str, reg: &str) -> Self {
        let reg = regex::Regex::new(reg).map_err(|e| anyhow!("invalid regex: {}", e)).unwrap();
        Self {
            description: description.to_string(),
            regex: reg,
        }
    }

    pub fn get_time_stamp(&self, line: &str) -> Result<SystemTime> {
        let re = self.regex.clone();
        let cap = re.captures(line).ok_or_else(|| anyhow!("No match"))?;

        // first, get the time zone offset
        let offset_seconds: i64 = 
            if let Some(_) = cap.name("TIMEZONE") {
                if let Some(_) = cap.name("UTC") {
                    0
                } else {
                    let offset = cap.name("OFFSET").ok_or_else(|| anyhow!("No match timezone offset"))?.as_str();
                    let hour = cap.name("TIMEZONE_HOUR").ok_or_else(|| anyhow!("No match timezone hour"))?.as_str().parse::<i32>()?;
                    let minute = cap.name("TIMEZONE_MINUTE").ok_or_else(|| anyhow!("No match timezone minute"))?.as_str().parse::<i32>()?;
                    let to_second = (hour * 3600 + minute * 60) as i64;
                    if offset == "+" {
                        -to_second // to UTC
                    } else if offset == "-" {
                        to_second
                    } else {
                        unreachable!("invalid time zone offset")
                    }
                }
            } else {
                // no time zone offset, consider it as local time zone
                -*TIME_ZONE_OFFSET_SECOND
            };
        let year = cap.name("YEAR").ok_or_else(|| anyhow!("No match year"))?.as_str().parse::<i32>()?;
        let month = cap.name("MONTH").ok_or_else(|| anyhow!("No match month"))?.as_str().parse::<u32>()?;
        let day = cap.name("DAY").ok_or_else(|| anyhow!("No match day"))?.as_str().parse::<u32>()?;

        let hour = cap.name("HOUR").ok_or_else(|| anyhow!("No match hour"))?.as_str().parse::<u32>()?;
        let minute = cap.name("MINUTE").ok_or_else(|| anyhow!("No match minute"))?.as_str().parse::<u32>()?;
        let second = cap.name("SECOND").ok_or_else(|| anyhow!("No match second"))?.as_str().parse::<u32>()?;

        let millisecond = cap.name("MILLISECOND").map_or(0, |m| m.as_str().parse::<i64>().unwrap_or(0));

        let time_stamp = millisecond + (offset_seconds * 1000) + chrono::Utc.with_ymd_and_hms(year, month, day, hour, minute, second).single().ok_or(anyhow!("invalid datetime"))?.timestamp_millis();
        Ok(SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(time_stamp as u64))
    } 

    pub fn is_match(&self, line: &str) -> bool {
        self.regex.is_match(line)
    }

    /// Get the time range of the file
    pub fn get_file_timerange(&self, file: &mut File) -> Result<((usize,SystemTime), (usize,SystemTime))> {
        let origin_pos = file.seek(std::io::SeekFrom::Current(0))?;
        let mut file_clone = file.try_clone()?;
        defer!(
            let _ = file.seek(std::io::SeekFrom::Start(origin_pos));
        );

        file_clone.seek(std::io::SeekFrom::Start(0))?;
        let buf_size = (file_clone.metadata()?.len().min(4096)) as i64;
        let mut buf = vec![0; buf_size as usize];

        let mut start_time_stamp = Option::<(usize, SystemTime)>::None; // (offset, time_stamp)
        let mut end_time_stamp = Option::<(usize, SystemTime)>::None; // (offset, time_stamp)

        let mut remaining_content: Vec<u8> = vec![];
        loop {
            if buf.len() == remaining_content.len(){
                remaining_content.clear(); 
            }
            // buf.copy_from_slice(&remaining_content);
            buf[0..remaining_content.len()].copy_from_slice(&remaining_content);
            file_clone.read_exact(&mut buf[remaining_content.len()..])?;
            let start_offset = file_clone.seek(std::io::SeekFrom::Current(0))? - buf.len() as u64;
            let new_lines_offsets = buf.iter().enumerate().filter(|(_, &b)| b == b'\n').map(|(i, _)| i);
            let mut start = 0;
            for index in new_lines_offsets{
                let line = &buf[start..index];

                let line = std::str::from_utf8(line)?;
                if self.is_match(line){
                    let time_stamp = self.get_time_stamp(line).unwrap();
                    start_time_stamp = Some((start_offset as usize + start, time_stamp));
                    break;
                }
                start = index + 1;
            }
            if start_time_stamp.is_some() {
                break;
            }
            remaining_content = buf[start..].to_vec();
        }
        // then find the last time stamp
        remaining_content.clear();
        file_clone.seek(std::io::SeekFrom::End(0))?;
        loop {
            if buf.len() == remaining_content.len(){
                remaining_content.clear(); 
            }
            buf[((buf_size as usize - remaining_content.len()) as usize)..].copy_from_slice(&remaining_content);
            let read_length = buf_size - remaining_content.len() as i64;
            file_clone.seek(std::io::SeekFrom::Current(-read_length))?;
            file_clone.read_exact(&mut buf[0..(read_length as usize)])?;
            let start_offset = file_clone.seek(std::io::SeekFrom::Current(0))? - read_length as u64;
            let new_lines_offsets = buf.iter().enumerate().rev().filter(|(_, &b)| b == b'\n').map(|(i, _)| i);
            let mut end = buf.len();
            for index in new_lines_offsets{
                let line = &buf[(index + 1)..end];
                end = index;
                let line = std::str::from_utf8(line)?;
                if self.is_match(line){
                    let time_stamp = self.get_time_stamp(line).unwrap();
                    end_time_stamp = Some((start_offset as usize + end, time_stamp));
                    break;
                }
            }
            if end_time_stamp.is_some() {
                break;
            }
        }
        Ok((start_time_stamp.ok_or(anyhow!("No match start time stamp"))?, end_time_stamp.ok_or(anyhow!("No match end time stamp"))?))


    }
    
}

static TIME_FORMAT_LIST: Lazy<Vec<TimeFormat>> = Lazy::new(||{
    vec![
        TimeFormat::new("RFC3339", r"(?P<YEAR>\d{4})\D(?P<MONTH>\d{2})\D(?P<DAY>\d{2})\D(?P<HOUR>\d{2})\D(?P<MINUTE>\d{2})\D(?P<SECOND>\d{2})(\.(?P<MILLISECOND>\d{3})){0,1}(?P<TIMEZONE>((?<OFFSET>[\+\-])(?P<TIMEZONE_HOUR>\d{2}):(?P<TIMEZONE_MINUTE>\d{2})|(?P<UTC>Z))){0,1}"),
    ]
});

pub fn detect_datetime_format(time_str: &str) -> Result<&'static TimeFormat> {
    for time_format in TIME_FORMAT_LIST.iter() {
        if time_format.regex.is_match(time_str) {
            return Ok(time_format);
        }
    }
    Err(anyhow!("No match datetime format"))
}


pub fn detect_file_time_format(file: &mut File) -> Result<&'static TimeFormat> {
    let origin_pos = file.seek(std::io::SeekFrom::Current(0))?;
    file.seek(std::io::SeekFrom::Start(origin_pos))?;
    let file_clone = file.try_clone()?;
    defer!(
        let _ = file.seek(std::io::SeekFrom::Start(origin_pos));
    );
    let mut line = String::new();
    let mut line_count = 0;
    let mut reader = BufReader::new(file_clone);

    fn try_match_all_time_format(line: &str) -> Result<Option<&'static TimeFormat>>{
        for time_format in TIME_FORMAT_LIST.iter() {
            if time_format.is_match(line) {
                return Ok(Some(time_format));
            } 
        }
        Ok(None)
    }
    while line_count < MAX_SAMPLEING_LINE {
        line.clear();
        reader.read_line(&mut line)?;
        if let Some(time_format) = try_match_all_time_format(&line)? {

            return Ok(time_format);
        }
        line_count += 1;
    }
    Err(anyhow!("No match datetime format"))
}



#[cfg(test)]
mod test{
    use chrono::DateTime;
    use chrono::Local;
    use chrono::NaiveDateTime;
    use super::*;
    use chrono::Utc;

    fn to_utc(naive_local: &NaiveDateTime) -> DateTime<Utc> {
        let local_dt = Local.from_local_datetime(naive_local).unwrap();
        local_dt.with_timezone(&Utc)
    }


    #[test]
    fn test_detect_format() -> Result<()>{
        let time_str = "2023-01-02 12:13:14.000";
        let time_format = detect_datetime_format(time_str)?;
        assert_eq!(time_format.description, "RFC3339");


        let time_str = "2023-01-02T12:13:14+08:00";
        let time_format = detect_datetime_format(time_str)?;
        assert_eq!(time_format.description, "RFC3339");

        Ok(())
    }

    #[test]
    fn test_get_time_stamp() -> Result<()>{
        let time_str = "2023-01-02 12:13:14.000"; // implicit local time zone
        let time_format = detect_datetime_format(time_str)?;
        let time_stamp = time_format.get_time_stamp(time_str)?;
        assert_eq!(time_stamp, SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(1672661594000 - (*TIME_ZONE_OFFSET_SECOND as u64)*1000));

        let time_str = "2023-01-02T20:13:14+08:00";
        let time_format = detect_datetime_format(time_str)?;
        let time_stamp = time_format.get_time_stamp(time_str)?;
        assert_eq!(time_stamp, SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(1672661594000));

        Ok(())
    }    

    #[test]
    fn test_detect_file_time_format() -> Result<()>{
        use super::*;
        use std::fs::File;
        let mut file = File::open("test/format1.log")?;
        let origin_pos = file.seek(std::io::SeekFrom::Current(0))?;
        let time_format = detect_file_time_format(&mut file)?;
        assert_eq!(time_format.description, "RFC3339");
        let current_pos = file.seek(std::io::SeekFrom::Current(0))?;
        assert_eq!(origin_pos, current_pos, "detect_file_time_format should not change the file position");
        Ok(())
    }

    #[test]
    fn test_get_file_range(){
        use super::*;
        use std::fs::File;
        let mut file = File::open("test/start_end.log").unwrap();
        let mut str = String::new();
        file.read_to_string(&mut str).unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();

        let lines = str.lines().collect::<Vec<_>>();

        let time_format = detect_file_time_format(&mut file).unwrap();
        let (start, end) = time_format.get_file_timerange(&mut file).unwrap();
        assert!(start.0 == 0);
        assert!(end.0 == 97);

        let start_naive = chrono::NaiveDateTime::parse_from_str(&lines[0][..19], "%Y-%m-%d %H:%M:%S").unwrap();
        let start_utc = to_utc(&start_naive);
        assert_eq!(start.1, start_utc.into());

        let end_naive = chrono::NaiveDateTime::parse_from_str(&lines.last().unwrap()[..19], "%Y-%m-%d %H:%M:%S").unwrap();
        let end_utc = to_utc(&end_naive);
        assert_eq!(end.1, end_utc.into());
    }
}