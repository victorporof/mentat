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

use mentat_core::{
    Entid,
};

use errors::{
    Result,
};

pub(crate) fn rewind_transaction(conn: &rusqlite::Connection, tx_id: Entid) -> Result<()> {
    // Here's how to do it without:
    // - updating caches
    // - addressing schema changes
    // - notifying watchers
    // Let's assume that's okay for now.
    conn.execute("DELETE FROM datoms WHERE rowid IN (SELECT d.rowid FROM transactions AS t, datoms AS d
      WHERE
         t.tx = ? AND
         t.e = d.e AND
         t.a = d.a AND
         t.value_type_tag = d.value_type_tag AND
         t.v = d.v)",
                 &[&tx_id])?;

    // TODO: we don't have enough information to determine the original tx at this point.  Maybe we
    // want to keep it around for Sync.next's use?  This means that the trivial expression
    // conn.execute("DELETE FROM datoms WHERE tx = ?", &[&tx_id])?;
    // does *NOT* do what we want, since we can't rewind multiple transactions with that approach.
    // TODO: figure out the correct flags here.
    conn.execute("INSERT INTO datoms (e, a, v, tx, value_type_tag) SELECT t.e, t.a, t.v, ?, t.value_type_tag FROM transactions AS t WHERE t.tx = ? AND t.added = 0", &[&tx_id, &tx_id])?;

    // If we wanted to eradicate our traces, as opposed to keeping old transactions on distinct
    // timelines.
    // conn.execute("DELETE FROM transactions WHERE t.tx = ?", &[&tx_id]);

    Ok(())
}

pub(crate) fn set_timeline(conn: &rusqlite::Connection, tx_ids: &[Entid], timeline: Entid) -> Result<()> {
    conn.execute(&format!("UPDATE transactions SET timeline = {} WHERE tx IN ({})", timeline, ::repeat_values(tx_ids.len(), 1)),
                 &(tx_ids.iter().map(|x| x as &rusqlite::types::ToSql).collect::<Vec<_>>()))?;
    Ok(())
}
