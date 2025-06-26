use std::{mem, sync::Arc};

use crate::{
    def::{OP_CREATE, OP_DELETE, OP_WRITE},
    tasks::Task,
    utils::{byte0_to_shard_id, changeset::ChangeSet, hasher},
};

#[derive(Debug)]
pub struct SingleCsTask {
    pub change_sets: Arc<Vec<ChangeSet>>,
}

impl Task for SingleCsTask {
    fn get_change_sets(&self) -> Arc<Vec<ChangeSet>> {
        self.change_sets.clone()
    }
}

pub struct TaskBuilder {
    /// The change set being built, containing all operations
    cs: ChangeSet,
}

impl Default for TaskBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskBuilder {
    pub fn new() -> Self {
        Self {
            cs: ChangeSet::new(),
        }
    }

    pub fn create(&mut self, k: &[u8], v: &[u8]) -> &mut Self {
        self.add_op(OP_CREATE, k, v)
    }

    pub fn write(&mut self, k: &[u8], v: &[u8]) -> &mut Self {
        self.add_op(OP_WRITE, k, v)
    }

    pub fn delete(&mut self, k: &[u8], v: &[u8]) -> &mut Self {
        self.add_op(OP_DELETE, k, v)
    }

    pub fn add_op(&mut self, op: u8, k: &[u8], v: &[u8]) -> &mut Self {
        let kh = hasher::hash(k);
        let shard_id = byte0_to_shard_id(kh[0]) as u8;
        self.cs.add_op(op, shard_id, &kh, k, v, Option::None);
        self
    }

    pub fn build(&mut self) -> SingleCsTask {
        self.cs.sort();
        let mut cs = ChangeSet::new();
        mem::swap(&mut cs, &mut self.cs);
        SingleCsTask {
            change_sets: Arc::new(vec![cs]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TaskBuilder;

    #[test]
    fn test_task_builder() {
        let task = TaskBuilder::new()
            .create(b"k1", b"v1")
            .create(b"k2", b"v2")
            .create(b"k3", b"v3")
            .build();
        assert_eq!(task.change_sets.len(), 1);
        assert_eq!(task.change_sets[0].op_list.len(), 3);
        println!("{:?}", task);
    }
}
