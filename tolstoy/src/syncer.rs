// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

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
use tx_uploader::TxUploader;
use tx_processor::{
    Processor,
};
use tx_mapper::{
    TxMapper,
};
use types::{
    Tx,
};

use logger::d;

pub struct Syncer {}

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
        let mut uploader = TxUploader::new(remote_client, remote_head);
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
        let (local_bootstrap_tx, local_head_tx) = SyncMetadataClient::root_and_head_tx(db_tx)?;
        // Empty means we just have a bootstrap transaction.
        let local_store_empty = local_bootstrap_tx == local_head_tx;
        // We "have changes" if there is a non-bootstrap transaction that hasn't been mapped.
        let have_local_changes = !local_store_empty && TxMapper::get(db_tx, local_head_tx)?.is_none();

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

            if incoming_txs.len() > 1 && !local_store_empty {
                bail!(TolstoyError::NotYetImplemented("Can't purge local state: local excision not supported yet".to_string()));
            }

            return Ok(SyncResult::LocalFastForward(incoming_txs[1 ..].to_vec()));

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
