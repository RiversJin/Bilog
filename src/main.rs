use std::{ops::{DerefMut, Deref}, time::SystemTime};

use clap::Parser;
use once_cell::sync::Lazy;

mod read;
use read::*;

use anyhow::{Result, anyhow};
#[derive(Debug, Parser)]
/// bilog -s '2023-01-02 20:13:14' -e '2023-01-02 20:13:14' -f /var/log/bi.log
/// Search the log file between 2023-01-02 20:13:14 and 2023-01-02 20:13:14
struct CLI {
    #[clap(short = 's', long, required = true, help = r#"Which datetime to start(includsive).
e.g. 
-s '2023-01-02 20:13:14', 
-s '2023/01/02 20:13:14', 
-s '2023-01-02T12:13:14', 
-s '2023-01-02T12:13:14.000', 
-s '2023-01-02T12:13:14.000Z',
-s '2023-01-02T12:13:14+08:00"#)]
    #[clap(short, long, required = true)]
    start_time: String,
    /// The end time of the time range 
    #[clap(short, long, required = false)]
    end_time: Option<String>,

    #[clap(required = true)]
    file: String
}

static CLI: Lazy<CLI> = Lazy::new(CLI::parse);
static START_TIME: Lazy<SystemTime> = Lazy::new(|| {
    let time_format = detect_datetime_format(&CLI.start_time).expect("No match datetime format");
    let time_stamp = time_format.get_time_stamp(&CLI.start_time).expect("invalid datetime");

    time_stamp
});

fn main() {
    println!("{:?}", &CLI.deref());

}
