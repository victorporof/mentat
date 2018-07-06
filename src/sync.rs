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
    KnownEntid,
};
use mentat_db::{
    renumber,
    PartitionMap,
};
use edn::entities::TempId;

use entity_builder::{
    BuildTerms,
    TermBuilder,
};

use mentat_tolstoy::{
    PartitionsTable,
    Syncer,
    SyncMetadata,
    SyncResult,
    Tx,
    TxMapper,
    TolstoyError,
    logger,
};

pub trait Syncable {
    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<()>;
}

fn fast_forward_local<'a, 'c>(in_progress: &mut InProgress<'a, 'c>, txs: Vec<Tx>) -> Result<Option<PartitionMap>> {
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
    let mut last_encountered_partition_map = None;

    for tx in txs {
        let mut builder = TermBuilder::new();

        last_encountered_partition_map = match tx.parts[0].partitions.clone() {
            Some(parts) => Some(parts),
            None => bail!(TolstoyError::BadServerState("Missing partition map in incoming transaction".to_string()))
        };

        for part in tx.parts {
            if part.added {
                builder.add(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
            } else {
                builder.retract(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
            }
        }

        let report = in_progress.transact_builder(builder)?;

        last_tx = Some((report.tx_id, tx.tx.clone()));
    }

    // We've just transacted a new tx, and generated a new tx entid.  Map it to the corresponding
    // incoming tx uuid, advance our "locally known remote head".
    if let Some((entid, uuid)) = last_tx {
        SyncMetadata::set_remote_head(&mut in_progress.transaction, &uuid)?;
        TxMapper::set_tx_uuid(&mut in_progress.transaction, entid, &uuid)?;
    }

    Ok(last_encountered_partition_map)
}

fn savepoint(db_tx: &rusqlite::Transaction, name: &String) -> Result<()> {
    db_tx.execute(&format!("SAVEPOINT {}", name), &[])?;
    Ok(())
}

fn rollback_savepoint(db_tx: &rusqlite::Transaction, name: &String) -> Result<()> {
    db_tx.execute(&format!("ROLLBACK TO SAVEPOINT {}", name), &[])?;
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
        let mut incoming_partition = None;

        match sync_result {
            SyncResult::EmptyServer => (),
            SyncResult::NoChanges => (),
            SyncResult::ServerFastForward => (),
            SyncResult::LocalFastForward(txs) => {
                incoming_partition = fast_forward_local(&mut in_progress, txs)?;
                ()
            },
            SyncResult::BadServerState => bail!(TolstoyError::NotYetImplemented(
                format!("Bad server state.")
            )),
            SyncResult::AdoptedRemoteOnFirstSync => (),
            SyncResult::IncompatibleBootstrapSchema => bail!(TolstoyError::NotYetImplemented(
                format!("IncompatibleBootstrapSchema.")
            )),
            SyncResult::Merge(incoming_txs, local_txs_to_merge) => {
                logger::d(&format!("Rewinding local transactions."));
                // 1) rewind local to shared root
                let mut builder = TermBuilder::new();
                let txs_to_rewind = local_txs_to_merge.clone();
                for local_tx in txs_to_rewind {
                    logger::d(&format!("Rewinding {}", local_tx.tx));
                    // Syncer::rewind_tx(&in_progress.transaction, local_tx.tx)?;

                    for part in local_tx.parts {
                        if part.added {
                            if part.a == 1 {
                                builder.retract(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
                            }
                        } else {
                            builder.add(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
                        }
                    }
                }
                let rewind_report = in_progress.transact_builder(builder)?;
                // We want the side-effects of rewinding (schema mutations, etc), not its effects (entries in transactions table).
                Syncer::eradicate_from_transactions(&in_progress.transaction, rewind_report.tx_id)?;
                for local_tx in &local_txs_to_merge {
                    Syncer::eradicate_from_transactions(&in_progress.transaction, local_tx.tx)?;
                }

                logger::d(&format!("Transacting incoming..."));
                // 2) play incoming onto local - same as fast-forward-local?
                let mut last_encountered_partition_map = None;
                let mut remote_report = None;
                for remote_tx in incoming_txs {
                    let mut builder = TermBuilder::new();

                    last_encountered_partition_map = match remote_tx.parts[0].partitions.clone() {
                        Some(parts) => Some(parts),
                        None => bail!(TolstoyError::BadServerState("Missing partition map in incoming transaction".to_string()))
                    };

                    for part in remote_tx.parts {
                        if part.added {
                            builder.add(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
                        } else {
                            builder.retract(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
                        }
                    }

                    remote_report = Some((in_progress.transact_builder(builder)?.tx_id, remote_tx.tx));
                }

                logger::d(&format!("Moving partitions after transacting incoming..."));
                let root = SyncMetadata::get_partitions(&in_progress.transaction, PartitionsTable::Core)?;
                let current = SyncMetadata::get_partitions(&in_progress.transaction, PartitionsTable::Tolstoy)?;
                let updated_db = renumber(&in_progress.transaction, &root, &current, &last_encountered_partition_map.unwrap())?;
                in_progress.partition_map = updated_db.partition_map;

                let mut local_applied_as_noops = true;
                
                // TODO we can't use in_progress.transaction.savepoint() because
                // we'd be double-borrowing in_progress as mutable when we commit a builder.
                // moving savepoint into in_progress is also non-trivial because of conflicting lifetimes
                logger::d(&format!("Savepoint before transacting local..."));
                let sp_name = "speculative_local".to_string();
                savepoint(&in_progress.transaction, &sp_name)?;

                logger::d(&format!("Transacting local on top of incoming..."));
                // play rewound local txs on top of remote, one by one
                let mut txs_to_eradicate = vec![];
                for local_tx in local_txs_to_merge {
                    let mut builder = TermBuilder::new();

                    for part in local_tx.parts {
                        let tempid = builder.named_tempid(format!("{}", part.e));
                        if part.added {
                            builder.add(tempid, KnownEntid(part.a), part.v)?;
                        } else {
                            builder.retract(tempid, KnownEntid(part.a), part.v)?;
                        }
                    }

                    logger::d(&format!("Transacting builder filled with local txs... {:?}", builder));
                    let report = in_progress.transact_builder(builder);
                    println!("report: {:?}", report);
                    let report = report?;
                    if !SyncMetadata::is_tx_empty(&in_progress.transaction, report.tx_id)? {
                        logger::d(&format!("Local is not a no-op, abort!"));
                        local_applied_as_noops = false;
                        break;
                    } else {
                        logger::d(&format!("Applied as a no-op!"));
                    }
                    txs_to_eradicate.push(local_tx.tx);
                }

                logger::d(&format!("Rolling back savepoint..."));
                rollback_savepoint(&in_progress.transaction, &sp_name)?;

                if !local_applied_as_noops {
                    bail!(TolstoyError::NotYetImplemented(
                        format!("Local has transactions which are not a subset of remote transactions.")
                    ))
                }

                // at this point, we have fully applied remote transactions and determined that local transactions could be discarded.
                // this is when create a "merge commit", move local txs that lost to a timeline referring to the commit, upload the commit and the timeline...
                // for now let's delete the loosing timeline.
                for tx in txs_to_eradicate {
                    logger::d(&format!("Eradicating local tx {}", tx));
                    Syncer::eradicate_from_transactions(&in_progress.transaction, tx)?;
                }

                if let Some((entid, uuid)) = remote_report {
                    SyncMetadata::set_remote_head(&mut in_progress.transaction, &uuid)?;
                    TxMapper::set_tx_uuid(&mut in_progress.transaction, entid, &uuid)?;
                }

                ()
            },
        }

        match incoming_partition {
            Some(incoming) => {
                let root = SyncMetadata::get_partitions(&in_progress.transaction, PartitionsTable::Core)?;
                let current = SyncMetadata::get_partitions(&in_progress.transaction, PartitionsTable::Tolstoy)?;
                let updated_db = renumber(&in_progress.transaction, &root, &current, &incoming)?;
                in_progress.partition_map = updated_db.partition_map;
                ()
            },
            None => ()
        }

        in_progress.commit()
    }
}
