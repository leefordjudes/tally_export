use std::{
    path::PathBuf,
    fs::{File, read_to_string},
    io::Write
};
use clap::Parser;
use chrono::NaiveDate;
use mongodb::{options::ClientOptions, Client};

mod export;
use export::*;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    uri: String,
    #[clap(short, long)]
    org: String,
    #[arg(short, long)]
    account_map: PathBuf,
    #[arg(short, long)]
    voucher_type_map: PathBuf,
    #[clap(short, long)]
    from_date: NaiveDate,
    #[clap(short, long)]
    to_date: NaiveDate,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let uri = args.uri;
    let org = args.org;
    let from_date = args.from_date;
    let to_date = args.to_date;
    let account_map = read_to_string(&args.account_map).unwrap();
    let voucher_type_map = read_to_string(&args.voucher_type_map).unwrap();
    // println!("uri: {:?}\norg:{:?},\nfd:{:?},\ntd:{:?},\nmap:{:?}", uri, org,from_date,to_date, map);
    let client_options = match ClientOptions::parse(&uri).await {
        Ok(options) => options,
        Err(_) => {
            panic!("Database connection failure");
        }
    };
    let client = Client::with_options(client_options).unwrap();
    let db = client.database(&org);
    let data = export_data(&db, account_map, voucher_type_map, from_date, to_date).await;
    let options = xml_serde::Options { include_schema_location: false };
    let res = xml_serde::to_string_custom(&data, options).unwrap();
    let mut file = File::create("tally_data.xml").unwrap();
    file.write_all(res.as_bytes()).unwrap();
}
