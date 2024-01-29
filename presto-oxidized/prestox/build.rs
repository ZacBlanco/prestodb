use quote::quote;
use std::path::Path;

const TIME_ZONE_INDEX_FILE: &str = "resources/zone-index.properties";
const OUTPUT_TIME_ZONE_DB: &str = "tz.rs";

fn main() {
    let output_path = std::path::Path::new(OUTPUT_TIME_ZONE_DB);
    if output_path.exists() {
        return;
    }
    let data =
        std::fs::read_to_string(TIME_ZONE_INDEX_FILE).expect("zone-index.properties should exist");
    let result = data
        .lines()
        .filter(|x| !x.starts_with("#"))
        .map(|x| {
            let parts = x.split(" ").collect::<Vec<_>>();
            (parts[0].parse::<i16>().expect("parse tzid"), parts[1])
        })
        .collect::<Vec<_>>();
    let ids = result.iter().map(|x| x.0).collect::<Vec<_>>();
    let vals = result.iter().map(|x| x.1).collect::<Vec<_>>();
    let function = quote! {
        pub fn time_zone_db(zone_id: i16) -> Option<&'static str> {
            match zone_id {
                #(#ids => Some(#vals),)*
                _ => None,
            }
        }

        pub fn time_zone_reverse_db(zone_id: &str) -> Option<i16> {
            match zone_id {
                #(#vals => Some(#ids),)*
                _ => None,
            }
        }
    };
    println!("{}", function.to_string());
    std::fs::write(
        Path::new(&std::env::var("OUT_DIR").unwrap()).join(OUTPUT_TIME_ZONE_DB),
        function.to_string(),
    )
    .expect("should be able to write file");
}
