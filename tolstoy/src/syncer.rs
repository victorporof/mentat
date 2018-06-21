// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;

use rusqlite;
use uuid::Uuid;

use mentat_core::{
    Entid,
};
use mentat_db::{
    CORE_SCHEMA_VERSION,
};
use bootstrap::{
    BootstrapHelper,
};
use errors::{
    TolstoyError,
    Result,
};
use metadata::{
    SyncMetadataClient,
};
use remote_client::{
    RemoteClient,
};
use schema::{
    ensure_current_version,
};
use tx_processor::{
    Processor,
    TxReceiver,
};
use tx_mapper::{
    TxMapper,
};
use types::{
    Tx,
    TxPart,
};

// TODO it would be nice to be able to pass
// in a logger into Syncer::flow; would allow for a "debug mode"
// and getting useful logs out of clients.
// See https://github.com/mozilla/mentat/issues/571
// Below is some debug Android-friendly logging:

// use std::os::raw::c_char;
// use std::os::raw::c_int;
// use std::ffi::CString;
// pub const ANDROID_LOG_DEBUG: i32 = 3;
// extern { pub fn __android_log_write(prio: c_int, tag: *const c_char, text: *const c_char) -> c_int; }

pub fn d(message: &str) {
    println!("d: {}", message);
    // let message = CString::new(message).unwrap();
    // let message = message.as_ptr();
    // let tag = CString::new("RustyToodle").unwrap();
    // let tag = tag.as_ptr();
    // unsafe { __android_log_write(ANDROID_LOG_DEBUG, tag, message) };
}

pub struct Syncer {}

// TODO this is sub-optimal, we don't need to walk the table
// to query the last thing in it w/ an index on tx!!
// but it's the hammer at hand!
// See https://github.com/mozilla/mentat/issues/572
struct InquiringTxReceiver {
    pub first_tx: Option<Entid>,
    pub last_tx: Option<Entid>,
    pub is_done: bool,
}

impl InquiringTxReceiver {
    fn new() -> InquiringTxReceiver {
        InquiringTxReceiver {
            first_tx: None,
            last_tx: None,
            is_done: false,
        }
    }
}

impl TxReceiver for InquiringTxReceiver {
    fn tx<T>(&mut self, tx_id: Entid, _datoms: &mut T) -> Result<()>
    where T: Iterator<Item=TxPart> {
        match self.first_tx {
            None => self.first_tx = Some(tx_id),
            Some(_) => ()
        }
        self.last_tx = Some(tx_id);
        Ok(())
    }

    fn done(&mut self) -> Result<()> {
        self.is_done = true;
        Ok(())
    }
}

struct UploadingTxReceiver<'c> {
    pub tx_temp_uuids: HashMap<Entid, Uuid>,
    pub is_done: bool,
    remote_client: &'c RemoteClient,
    remote_head: &'c Uuid,
    rolling_temp_head: Option<Uuid>,
}

impl<'c> UploadingTxReceiver<'c> {
    fn new(client: &'c RemoteClient, remote_head: &'c Uuid) -> UploadingTxReceiver<'c> {
        UploadingTxReceiver {
            tx_temp_uuids: HashMap::new(),
            remote_client: client,
            remote_head: remote_head,
            rolling_temp_head: None,
            is_done: false
        }
    }
}

impl<'c> TxReceiver for UploadingTxReceiver<'c> {
    fn tx<T>(&mut self, tx_id: Entid, datoms: &mut T) -> Result<()>
    where T: Iterator<Item=TxPart> {
        // Yes, we generate a new UUID for a given Tx, even if we might
        // already have one mapped locally. Pre-existing local mapping will
        // be replaced if this sync succeeds entirely.
        // If we're seeing this tx again, it implies that previous attempt
        // to sync didn't update our local head. Something went wrong last time,
        // and it's unwise to try to re-use these remote tx mappings.
        // We just leave garbage txs to be GC'd on the server.
        let tx_uuid = Uuid::new_v4();
        self.tx_temp_uuids.insert(tx_id, tx_uuid);
        let mut tx_chunks = vec![];

        // TODO separate bits of network work should be combined into single 'future'

        // Upload all chunks.
        for datom in datoms {
            let datom_uuid = Uuid::new_v4();
            tx_chunks.push(datom_uuid);
            d(&format!("putting chunk: {:?}, {:?}", &datom_uuid, &datom));
            // TODO switch over to CBOR once we're past debugging stuff.
            // See https://github.com/mozilla/mentat/issues/570
            // let cbor_val = serde_cbor::to_value(&datom)?;
            // self.remote_client.put_chunk(&datom_uuid, &serde_cbor::ser::to_vec_sd(&cbor_val)?)?;
            self.remote_client.put_chunk(&datom_uuid, &datom)?;
        }

        // Upload tx.
        // NB: At this point, we may choose to update remote & local heads.
        // Depending on how much we're uploading, and how unreliable our connection
        // is, this might be a good thing to do to ensure we make at least some progress.
        // Comes at a cost of possibly increasing racing against other clients.
        match self.rolling_temp_head {
            Some(parent) => {
                d(&format!("putting transaction: {:?}, {:?}, {:?}", &tx_uuid, &parent, &tx_chunks));
                self.remote_client.put_transaction(&tx_uuid, &parent, &tx_chunks)?;
                
            },
            None => {
                d(&format!("putting transaction: {:?}, {:?}, {:?}", &tx_uuid, &self.remote_head, &tx_chunks));
                self.remote_client.put_transaction(&tx_uuid, self.remote_head, &tx_chunks)?;
            }
        }

        d(&format!("updating rolling head: {:?}", tx_uuid));
        self.rolling_temp_head = Some(tx_uuid.clone());

        Ok(())
    }

    fn done(&mut self) -> Result<()> {
        self.is_done = true;
        Ok(())
    }
}

pub enum SyncResult {
    BadServerState,
    EmptyServer,
    NoChanges,
    ServerFastForward,
    AdoptedRemoteOnFirstSync,
    IncompatibleBootstrapSchema,
    LocalFastForward(Vec<Tx>),
    Merge,
}

impl Syncer {
    fn fast_forward_server(db_tx: &mut rusqlite::Transaction, from_tx: Option<Entid>, remote_client: &RemoteClient, remote_head: &Uuid) -> Result<()> {
        let mut uploader = UploadingTxReceiver::new(remote_client, remote_head);
        Processor::process(db_tx, from_tx, &mut uploader)?;
        if !uploader.is_done {
            bail!(TolstoyError::TxProcessorUnfinished);
        }
        // Last tx uuid uploaded by the tx receiver.
        // It's going to be our new head.
        if let Some(last_tx_uploaded) = uploader.rolling_temp_head {
            // Upload remote head.
            remote_client.put_head(&last_tx_uploaded)?;

            // On success:
            // - persist local mappings from the receiver
            // - update our local "remote head".
            TxMapper::set_bulk(db_tx, &uploader.tx_temp_uuids)?;
            SyncMetadataClient::set_remote_head(db_tx, &last_tx_uploaded)?;
        }

        Ok(())
    }

    pub fn flow(db_tx: &mut rusqlite::Transaction, server_uri: &String, user_uuid: &Uuid) -> Result<SyncResult> {
        d(&format!("sync flowing"));

        ensure_current_version(db_tx)?;
        
        // TODO configure this sync with some auth data
        let remote_client = RemoteClient::new(server_uri.clone(), user_uuid.clone());

        let remote_head = remote_client.get_head()?;
        d(&format!("remote head {:?}", remote_head));

        let locally_known_remote_head = SyncMetadataClient::remote_head(db_tx)?;
        d(&format!("local head {:?}", locally_known_remote_head));

        // Local head: latest transaction that we have in the store,
        // but with one caveat: its tx might will not be mapped if it's
        // never been synced successfully.
        // In other words: if latest tx isn't mapped, then HEAD moved
        // since last sync and server needs to be updated.
        let mut inquiring_tx_receiver = InquiringTxReceiver::new();
        // TODO we really just care about our first tx (bootstrap) and our HEAD.
        // we don't need to walk 'transactions' to figure this out!
        Processor::process(db_tx, None, &mut inquiring_tx_receiver)?;
        if !inquiring_tx_receiver.is_done {
            bail!(TolstoyError::TxProcessorUnfinished);
        }
        let (have_local_changes, local_store_empty) = match inquiring_tx_receiver.last_tx {
            Some(tx) => {
                match TxMapper::get(db_tx, tx)? {
                    Some(_) => (false, false),
                    None => (true, false)
                }
            },
            None => (false, true)
        };
        let local_bootstrap_tx = match inquiring_tx_receiver.first_tx {
            Some(tx) => tx,
            None => bail!(TolstoyError::UnexpectedState("Missing local bootstrap transaction.".to_string()))
        };

        // Server is empty - populate it.
        if remote_head == Uuid::nil() {
            d(&format!("empty server!"));
            Syncer::fast_forward_server(db_tx, None, &remote_client, &remote_head)?;
            return Ok(SyncResult::EmptyServer);

        // Server is not empty, and we never synced.
        // Reconcile bootstrap transaction and adopt remote state.
        } else if locally_known_remote_head == Uuid::nil() {
            d(&format!("server non-empty on first sync, adopting remote state."));

            let incoming_txs = remote_client.get_transaction_data_after(&locally_known_remote_head)?;

            if incoming_txs.len() == 0 {
                bail!(TolstoyError::BadServerState("Server specified non-root HEAD but gave no transactions".to_string()));
            }

            if incoming_txs.len() > 1 {
                bail!(TolstoyError::NotYetImplemented("Can't purge local state: local excision not supported yet".to_string()));
            }

            // We assume that the first transaction encountered on first sync is the bootstrap transaction.
            let bootstrap_tx = &incoming_txs[0];
            let remote_bootstrap = BootstrapHelper::new(bootstrap_tx);
            
            if !remote_bootstrap.is_compatible()? {
                bail!(TolstoyError::IncompatibleBootstrapSchema(CORE_SCHEMA_VERSION as i64, remote_bootstrap.core_schema_version()?));
            }

            d(&format!("mapping incoming bootstrap tx uuid to local bootstrap entid: {} -> {}", bootstrap_tx.tx, local_bootstrap_tx));

            // Map incoming bootstrap tx uuid to local bootstrap entid.
            // If there's more work to do, we'll move the head again.
            SyncMetadataClient::set_remote_head(db_tx, &bootstrap_tx.tx)?;
            TxMapper::set_tx_uuid(db_tx, local_bootstrap_tx, &bootstrap_tx.tx)?;

            return Ok(SyncResult::AdoptedRemoteOnFirstSync);

        // Server did not change since we last talked to it.
        } else if locally_known_remote_head == remote_head {
            d(&format!("server unchanged since last sync."));
            
            // Trivial case: our HEAD did not move.
            if !have_local_changes {
                d(&format!("local HEAD did not move. Nothing to do!"));
                return Ok(SyncResult::NoChanges);
            }

            // Our HEAD moved. Fast-forward server by uploading everything locally that is new.
            d(&format!("local HEAD moved."));
            // TODO it's possible that we've successfully advanced remote head previously,
            // but failed to advance our own local head. If that's the case, and we can recognize it,
            // our sync becomes just bumping our local head. AFAICT below would currently fail.
            if let Some(upload_from_tx) = TxMapper::get_tx_for_uuid(db_tx, &locally_known_remote_head)? {
                d(&format!("Fast-forwarding the server."));
                Syncer::fast_forward_server(db_tx, Some(upload_from_tx), &remote_client, &remote_head)?;
                return Ok(SyncResult::ServerFastForward);
            } else {
                d(&format!("Unable to fast-forward the server; missing local tx mapping"));
                bail!(TolstoyError::TxIncorrectlyMapped(0));
            }
            
        // Server changed since we last talked to it.
        } else {
            d(&format!("server changed since last sync."));

            // TODO local store moved forward since we last synced. Need to merge or rebase.
            if !local_store_empty && have_local_changes {
                return Ok(SyncResult::Merge);
            }

            d(&format!("fast-forwarding local store."));
            return Ok(SyncResult::LocalFastForward(
                remote_client.get_transaction_data_after(&locally_known_remote_head)?
            ));
        }

        // Our caller will commit the tx with our changes when it's done.
    }
}
