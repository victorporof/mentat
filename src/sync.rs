// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use uuid::Uuid;

use rusqlite;

use conn::{
    Conn,
    InProgress,
};

use errors::{
    Result,
};

use mentat_core::{
    Entid,
    KnownEntid,
};
use mentat_db::{
    USER0,
    TX0,
};
use mentat_db::db;
use mentat_db::db::{
    PartitionMapping,
};

use entity_builder::{
    BuildTerms,
    TermBuilder,
};

use mentat_tolstoy::{
    Syncer,
    SyncMetadataClient,
    SyncResult,
    Tx,
    TxMapper,
    TolstoyError,
};

pub trait Syncable {
    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<()>;
}

fn within_user_partition(entid: Entid) -> bool {
    entid >= USER0 && entid < TX0
}

fn fast_forward_local<'a, 'c>(in_progress: &mut InProgress<'a, 'c>, txs: Vec<Tx>) -> Result<()> {
    let mut last_tx = None;

    // During fast-forwarding, we will insert datoms with known entids
    // which, by definition, fall outside of our user partition.
    // Once we've done with insertion, we need to ensure that user
    // partition's next allocation will not overlap with just-inserted datoms.
    // To allow for "holes" in the user partition (due to data excision),
    // we track the highest incoming entid we saw, and expand our
    // local partition to match.
    // In absence of excision and implementation bugs, this should work
    // just as if we counted number of incoming entids and expanded by
    // that number instead.
    let mut largest_entid_encountered = USER0;

    for tx in txs {
        let mut builder = TermBuilder::new();

        for part in tx.parts {
            if part.added {
                builder.add(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
            } else {
                builder.retract(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
            }
            // Ignore datoms that fall outside of the user partition:
            if within_user_partition(part.e) && part.e > largest_entid_encountered {
                largest_entid_encountered = part.e;
            }
        }

        let report = in_progress.transact_builder(builder)?;

        last_tx = Some((report.tx_id, tx.tx.clone()));
    }

    // We've just transacted a new tx, and generated a new tx entid.  Map it to the corresponding
    // incoming tx uuid, advance our "locally known remote head".
    if let Some((entid, uuid)) = last_tx {
        SyncMetadataClient::set_remote_head(&mut in_progress.transaction, &uuid)?;
        TxMapper::set_tx_uuid(&mut in_progress.transaction, entid, &uuid)?;

        // We only need to advance the user partition, since we're using KnownEntid and partition
        // won't get auto-updated; shouldn't be a problem for tx partition, since we're relying on
        // the builder to create a tx and advance the partition for us.
        in_progress.partition_map.expand_up_to(":db.part/user", largest_entid_encountered);
        db::update_partition_map(&mut in_progress.transaction, &in_progress.partition_map)?;
    }

    Ok(())
}

impl Conn {
    pub(crate) fn sync(&mut self,
                       sqlite: &mut rusqlite::Connection,
                       server_uri: &String, user_uuid: &String) -> Result<()> {
        let uuid = Uuid::parse_str(&user_uuid)?;

        // Take an IMMEDIATE transaction right away.  We have an SQL transaction, and complete
        // control over the `Conn` metadata at this point, just like `transact()`.
        let mut in_progress = self.begin_transaction(sqlite)?;

        let sync_result = Syncer::flow(&mut in_progress.transaction, server_uri, &uuid)?;

        match sync_result {
            SyncResult::EmptyServer => (),
            SyncResult::NoChanges => (),
            SyncResult::ServerFastForward => (),
            SyncResult::Merge => bail!(TolstoyError::NotYetImplemented(
                format!("Can't sync against diverged local.")
            )),
            SyncResult::LocalFastForward(txs) => fast_forward_local(&mut in_progress, txs)?,
            SyncResult::BadServerState => bail!(TolstoyError::NotYetImplemented(
                format!("Bad server state.")
            )),
            SyncResult::AdoptedRemoteOnFirstSync => (),
            SyncResult::IncompatibleBootstrapSchema => bail!(TolstoyError::NotYetImplemented(
                format!("IncompatibleBootstrapSchema.")
            )),
        }

        in_progress.commit()
    }
}
