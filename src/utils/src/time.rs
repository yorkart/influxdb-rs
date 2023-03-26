use chrono::format::StrftimeItems;
use chrono::NaiveDateTime;

pub fn unix_nano_to_time(unix_nano: i64) -> NaiveDateTime {
    let secs = unix_nano / 1000000000;
    let nsecs = unix_nano - secs * 1000000000;
    NaiveDateTime::from_timestamp_opt(secs, nsecs as u32).unwrap()
}

pub fn time_format(dt: NaiveDateTime) -> String {
    let fmt = StrftimeItems::new("%Y-%m-%d %H:%M:%S");
    format!("{}", dt.format_with_items(fmt))
}
