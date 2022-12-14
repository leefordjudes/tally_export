use std::cmp::Ordering;
use serde::{Serialize, Deserialize};
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use futures::TryStreamExt;
use mongodb::{
    Database,
    bson::{doc, Document, from_document}
};
use mongodb::options::{AggregateOptions, FindOptions};


#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct ExportData {
    pub envelope: Envelope
}
impl ExportData {
    pub fn new(envelope: Envelope) -> Self {
        Self { envelope }
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct Envelope {
    pub body: Body
}
impl Envelope {
    pub fn new(body: Body) -> Self {
        Self { body }
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct Body {
    #[serde(rename = "IMPORTDATA")]
    pub import_data: ImportData
}
impl Body {
    pub fn new(import_data: ImportData) -> Self {
        Self { import_data }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct ImportData {
    #[serde(rename = "REQUESTDATA")]
    pub request_data: RequestData
}
impl ImportData {
    pub fn new(request_data: RequestData) -> Self {
        Self { request_data }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct RequestData {
    #[serde(rename = "TALLYMESSAGE")]
    pub items: Vec<TallyMessage>
}
impl RequestData {
    pub fn new(items: Vec<TallyMessage>) -> Self {
        Self { items }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct TallyMessage {
    #[serde(rename = "VOUCHER")]
    pub items: Vec<Voucher>
}
impl TallyMessage {
    pub fn new(voucher: Voucher) -> Self {
        Self {items:vec![voucher] }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct LedgerEntry {
    #[serde(rename = "LEDGERNAME")]
    pub ledger_name: String,
    #[serde(rename = "ISDEEMEDPOSITIVE")]
    pub is_deemed_positive: String,
    pub amount: f64,
}

impl LedgerEntry {
    pub fn new(ledger_name: String, amount: f64) -> Self {
        Self {
            ledger_name,
            is_deemed_positive: if amount < 0.0 { "Yes".to_string() } else {"No".to_string()},
            amount,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct Voucher {
    pub date: String,
    #[serde(rename = "REFERENCE", skip_serializing_if = "Option::is_none")]
    pub ref_no: Option<String>,
    #[serde(rename = "REFERENCEDATE", skip_serializing_if = "Option::is_none")]
    pub ref_date: Option<String>,
    #[serde(rename = "VOUCHERTYPENAME")]
    pub voucher_type: String,
    #[serde(rename = "PARTYLEDGERNAME")]
    pub party_ledger: String,
    #[serde(rename = "VOUCHERNUMBER")]
    pub voucher_no: String,
    #[serde(rename = "ALLLEDGERENTRIES.LIST")]
    pub ledger_entries: Vec<LedgerEntry>,
}

impl Voucher {
    pub fn new(date:String, ref_no: Option<String>, ref_date: Option<String>, voucher_type: String, party_ledger: String, voucher_no: String, ledger_entries: Vec<LedgerEntry>) -> Self {
        Self {date, ref_no, ref_date, voucher_type, party_ledger, voucher_no, ledger_entries}
    }
}

#[derive(Debug, Deserialize)]
pub struct NameMap {
    pub auditplus: String,
    pub tally: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub account: String,
    pub amount: f64,
    pub account_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AGVoucher {
    pub date: String,
    pub bill_date: Option<String>,
    pub ref_no: Option<String>,
    pub narration: Option<String>,
    pub voucher_type: String,
    pub voucher_no: String,
    pub trns: Vec<Transaction>,
    pub lut: Option<bool>,
    pub rcm: Option<bool>,
}

fn cmp_f64(a: &f64, b: &f64) -> Ordering {
    if a < b {
        return Ordering::Less;
    } else if a > b {
        return Ordering::Greater;
    }
    return Ordering::Equal;
}

fn get_voucher_type(voucher_type: &str, voucher_type_map:&Vec<NameMap>) -> String {
    let vtype = match voucher_type {
        "SALE" => "Sales".to_string(),
        "CREDIT_NOTE" => "Credit Note".to_string(),
        "PURCHASE" => "Purchase".to_string(),
        "DEBIT_NOTE" => "Debit Note".to_string(),
        "PAYMENT" => "Payment".to_string(),
        "RECEIPT" => "Receipt".to_string(),
        "JOURNAL" => "Journal".to_string(),
        "CONTRA" => "Contra".to_string(),
        _ => panic!("Invalid voucher type found"),
    };
    let voucher_type_name = if let Some(name) = voucher_type_map.iter().find(|x|x.auditplus == vtype) {
        name.tally.clone()
    } else {
        vtype
    };
    voucher_type_name
}

fn get_name_map(map_str: String) -> Vec<NameMap> {
    let mut alias: Vec<NameMap> = Vec::new();
    let mut rdr = csv::Reader::from_reader(map_str.as_bytes());
    for result in rdr.deserialize() {
        let record: NameMap = result.unwrap();
        alias.push(record);
    }
    alias
}

pub async fn export_data(db: &Database, account_map_str: String, voucher_type_map_str: String, from_date: NaiveDate, to_date: NaiveDate) -> ExportData {
    let date_time = from_date.and_time(NaiveTime::from_hms(0, 0, 0));
    let from_date = Utc.from_utc_datetime(&date_time);
    let date_time = to_date.and_time(NaiveTime::from_hms(0, 0, 0));
    let to_date = Utc.from_utc_datetime(&date_time);

    let account_map = get_name_map(account_map_str);
    let voucher_type_map = get_name_map(voucher_type_map_str);
    //"voucherType":{"$in":["CONTRA","PAYMENT","RECEIPT","JOURNAL"]}
    let pipeline = vec![
        doc! {"$match": {"date": { "$gte": from_date, "$lte": to_date }}},
        doc! {"$project": {
                "_id": 0,
                "voucherNo": 1,
                "voucherType": 1,
                "refNo": 1,
                // "date": "20220401",
                "date": {"$dateToString": { "format": "%Y%m%d", "date": "$date" }},
                // "billDate": "20220401",
                "billDate": {"$dateToString": { "format": "%Y%m%d", "date": "$billDate" }},
                "trns": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$acTrns",
                                "as": "trn",
                                "cond": { "$ne": ["$$trn.accountType", "STOCK"] }
                            }
                        },
                        "as": "trn",
                        "in": {
                            "account": {"$toString":"$$trn.account"},
                            "accountType": "$$trn.accountType",
                            "amount": { "$subtract": ["$$trn.credit", "$$trn.debit"] },
                        }
                    }
                },
                "rcm": 1,
                "lut": 1,
                "description": 1,
        }}
    ];
    let find_options = FindOptions::builder().projection(doc!{"_id":0,"name":1,"id": {"$toString":"$_id"}}).build();
    let accounts = db.collection::<Document>("accounts")
        .find(doc!{}, find_options)
        .await
        .unwrap()
        .try_collect::<Vec<Document>>()
        .await
        .unwrap();
    let mut tally_messages = Vec::new();
    let options = AggregateOptions::builder().allow_disk_use(true).build();
    let collections = vec!["vouchers","sales","purchases","gst_vouchers"];
    for collection in collections {
        let vouchers = db
            .collection::<Document>(collection)
            .aggregate(pipeline.clone(), options.clone())
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|x| Some(from_document::<AGVoucher>(x).unwrap()))
            .collect::<Vec<AGVoucher>>();
        for voucher in vouchers.iter() {
            let date = voucher.date.to_string();
            let voucher_type_name = get_voucher_type(voucher.voucher_type.as_str(), &voucher_type_map);
            let voucher_no = voucher.voucher_no.clone();
            let ref_no = voucher.ref_no.clone();
            let ref_date = voucher.bill_date.clone();
            let mut party_ledger_name = String::new();
            let mut ledger_entries = Vec::new();
            let mut party_ledgers = Vec::new();
            for trn in voucher.trns.iter() {
                let account_doc = accounts.iter().find(|x| x.get_str("id").unwrap() == trn.account).unwrap();
                let account_name = account_doc.get_str("name").unwrap().to_string();
                let account_name = if let Some(name) = account_map.iter().find(|x|x.auditplus == account_name) {
                    name.tally.clone()
                } else {
                    account_name
                };
                let amount = trn.amount as f64;
                let ledger = LedgerEntry::new(account_name.clone(), amount);
                ledger_entries.push(ledger.clone());
                if ["Contra", "Receipt"].contains(&voucher_type_name.as_str()) && amount > 0.0 {
                    party_ledger_name = account_name.clone();
                }
                if ["Payment"].contains(&voucher_type_name.as_str()) && amount < 0.0 {
                    party_ledger_name = account_name.clone();
                }
                if ["TRADE_RECEIVABLE","TRADE_PAYABLE","ACCOUNT_RECEIVABLE","ACCOUNT_PAYABLE","CASH","BANK_ACCOUNT","BANK_OD_ACCOUNT","EFT_ACCOUNT"].contains(&trn.account_type.as_str()) {
                    let mut party_ledger = ledger.clone();
                    party_ledger.amount = party_ledger.amount.abs();
                    party_ledgers.push(party_ledger);
                }
            }
            if ["Journal"].contains(&voucher_type_name.as_str())  {
                party_ledger_name = ledger_entries.first().clone().unwrap().ledger_name.clone();
            }
            if ["Sales", "Purchase", "Credit Note", "Debit Note" ].contains(&voucher_type_name.as_str()) {
                party_ledgers.sort_by(|a, b| cmp_f64(&b.amount, &a.amount));
                party_ledger_name = party_ledgers.first().clone().unwrap().ledger_name.clone();
                party_ledgers.clear();
            }
            if !["Journal"].contains(&voucher_type_name.as_str()) {
                ledger_entries.sort_by(|a, b| cmp_f64(&b.amount, &a.amount));
            }
            let voucher = Voucher::new(date, ref_no, ref_date,voucher_type_name, party_ledger_name, voucher_no, ledger_entries);
            let tally_message = TallyMessage::new(voucher);
            tally_messages.push(tally_message);
        }
    }
    let req_data = RequestData::new(tally_messages);
    let imp_data = ImportData::new(req_data);
    let body = Body::new(imp_data);
    let env = Envelope::new(body);
    ExportData::new(env)
}
