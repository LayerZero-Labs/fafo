use super::task::Task;
use crate::utils::changeset::ChangeSet;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::iter::repeat_with;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic::Ordering, Arc};

pub struct TasksManager<T: Task> {
    tasks: Vec<RwLock<Option<T>>>,
    valid_count: AtomicUsize,
}

impl<T: Task> TasksManager<T> {
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self {
            tasks: vec![],                    // include end_block task.
            valid_count: AtomicUsize::new(0), // include end_block task.
        }
    }

    pub fn with_size(size: usize) -> Self {
        let tasks = repeat_with(|| RwLock::new(None)).take(size).collect();
        Self {
            tasks,
            valid_count: AtomicUsize::new(0),
        }
    }

    pub fn new(tasks: Vec<RwLock<Option<T>>>, valid_count: usize) -> Self {
        Self {
            tasks,
            valid_count: AtomicUsize::new(valid_count),
        }
    }

    // contain end_block task.
    pub fn tasks_len(&self) -> usize {
        self.tasks.len()
    }

    pub fn get_valid_count(&self) -> usize {
        self.valid_count.load(Ordering::SeqCst)
    }

    pub fn set_valid_count(&self, count: usize) {
        self.valid_count.store(count, Ordering::SeqCst);
    }

    pub fn get_tasks_change_sets(&self, idx: usize) -> Arc<Vec<ChangeSet>> {
        let task_opt = self.tasks[idx].read();
        let task = task_opt.as_ref().unwrap();
        task.get_change_sets()
    }

    pub fn task_for_read(&self, idx: usize) -> RwLockReadGuard<Option<T>> {
        if idx >= self.tasks.len() {
            panic!("task index out of range");
        }
        self.tasks[idx].read()
    }
    pub fn task_for_write(&self, idx: usize) -> RwLockWriteGuard<Option<T>> {
        if idx >= self.tasks.len() {
            panic!("task index out of range");
        }
        self.tasks[idx].write()
    }
    pub fn set_task(&self, idx: usize, task: T) {
        let mut out_ptr = self.task_for_write(idx);
        assert!(out_ptr.is_none(), "task already exists");
        *out_ptr = Some(task);
    }
}
