use clap::builder::Str;
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::Database;
use serde::{Deserialize, Serialize};
use crate::NameMap;

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
    #[serde(rename = "LEDGER")]
    pub items: Vec<Account>
}
impl TallyMessage {
    pub fn new(ledger: Account) -> Self {
        Self {items:vec![ledger] }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct Name {
    #[serde(rename = "NAME")]
    pub lang_name: String
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct LanguageName {
    #[serde(rename = "NAME.LIST")]
    pub language_name: Name
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub struct Account {
    #[serde(rename = "LEDGER")]
    pub account: String,
    #[serde(rename = "PARENT")]
    pub account_type: String,
    #[serde(rename = "LANGUAGENAME.LIST")]
    pub language_name: LanguageName
}

impl Account {
    pub fn new(account: String, account_type: String, language_name: LanguageName) -> Self {
        Self {
            account,
            account_type,
            language_name
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CAccount {
    pub name: String,
    pub account_type: String
}

fn get_account_type(account_type: &str, account_type_map:&Vec<NameMap>) -> String {
    let acc_type = match account_type {
        "DIRECT_INCOME" => "Direct Income".to_string(),
        "INDIRECT_INCOME" => "Indirect Income".to_string(),
        "SALE" => "Sale".to_string(),
        "DIRECT_EXPENSE" => "Direct Expense".to_string(),
        "INDIRECT_EXPENSE" => "Indirect Expense".to_string(),
        "PURCHASE" => "Purchase".to_string(),
        "FIXED_ASSET" => "Fixed Asset".to_string(),
        "CURRENT_ASSET" => "Current Asset".to_string(),
        "LONGTERM_LIABILITY" => "LongTerm Liability".to_string(),
        "CURRENT_LIABILITY" => "Current Liability".to_string(),
        "EQUITY" => "Equity".to_string(),
        "CASH" => "Cash".to_string(),
        "STOCK" => "Stock".to_string(),
        "UNDEPOSITED_FUNDS" => "Undeposited Funds".to_string(),
        "BANK_ACCOUNT" => "Bank Account".to_string(),
        "BANK_OD_ACCOUNT" => "Bank OD Account".to_string(),
        "GST_PAYABLE" => "Direct Income".to_string(),
        "GST_RECEIVABLE" => "Indirect Income".to_string(),
        "EFT_ACCOUNT" => "Sale".to_string(),
        "PAYABLE" => "Payable".to_string(),
        "RECEIVABLE" => "Receivable".to_string(),
        "ACCOUNT_PAYABLE" => "Account Payable".to_string(),
        "ACCOUNT_RECEIVABLE" => "Account Receivable".to_string(),
        "TRADE_PAYABLE" => "Trade Payable".to_string(),
        "TRADE_RECEIVABLE" => "Trade Receivable".to_string(),
        "BRANCH_TRANSFER" => "Branch Transfer".to_string(),
        _ => panic!("Invalid account type found"),
    };
    let account_type_name = if let Some(acc_name) = account_type_map.iter().find(|x|x.auditplus == acc_type) {
        acc_name.tally.clone()
    } else {
        acc_type
    };
    account_type_name
}

pub async fn export_ledger(db: &Database, account_type_map_str: String, account_name: String) -> ExportData {
    let account_type_map = crate::get_name_map(account_type_map_str);
    let account = db.collection::<CAccount>("accounts")
        .find(doc! {"name": &account_name}, None).await.unwrap().try_collect::<Vec<CAccount>>().await.unwrap();
    let mut tally_messages = Vec::new();

    for acc in account {
    let acc_type = get_account_type(acc.account_type.as_str(), &account_type_map);
    let acc = acc.name;
    let lang_name = LanguageName {
        language_name: Name {
            lang_name: acc.clone()
        }
    };
    let account = Account::new(acc, acc_type, lang_name);
    let tally_message = TallyMessage::new(account);
    tally_messages.push(tally_message);
}
    let req_data = RequestData::new(tally_messages);
    let imp_data = ImportData::new(req_data);
    let body = Body::new(imp_data);
    let env = Envelope::new(body);
    ExportData::new(env)
}