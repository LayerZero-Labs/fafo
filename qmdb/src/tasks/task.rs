use std::sync::Arc;

use crate::{entryfile::EntryCache, utils::changeset::ChangeSet};

pub trait Task: Send + Sync {
    fn get_change_sets(&self) -> Arc<Vec<ChangeSet>>;
}

pub trait TaskHub: Send + Sync {
    fn get_entry_cache(&self, height: i64) -> Arc<EntryCache>;
    fn get_change_sets(&self, task_id: i64) -> Arc<Vec<ChangeSet>>;
}
