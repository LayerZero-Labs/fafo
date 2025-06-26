use crate::exetask::ExeTask;
use qmdb::ADS;

use super::{
    dashboard::{Dashboard, FIRST_FRAME},
    def::EARLY_EXE_WINDOW_SIZE,
    dtask::DTask,
};

pub trait Dispatchable: Send + Sync + Clone {
    fn warm_up(&self) -> i32;
    fn execute(&self);
    fn get_dashboard(&self) -> &Dashboard;
    fn get_idx(&self) -> i32;
    fn get_sibling(&self, idx: i32) -> Self;
    fn end_block(&self);
}

impl<T: ADS> Dispatchable for DTask<T> {
    fn warm_up(&self) -> i32 {
        let my_idx = self.idx as usize;

        if !self.blk_ctx.with_simulator {
            self.blk_ctx.warmup(my_idx);
        }

        let task_opt = self.blk_ctx.tasks_manager.task_for_read(my_idx);
        let task = task_opt.as_ref().unwrap();

        let task_out_start = task.get_task_out_start();
        if task_out_start == 0 {
            return FIRST_FRAME;
        }
        let mut stop_detect = 0;
        if my_idx > EARLY_EXE_WINDOW_SIZE {
            stop_detect = my_idx - EARLY_EXE_WINDOW_SIZE;
        }
        let mut eei = if stop_detect == 0 { 0 } else { stop_detect - 1 };
        // find the smallest early_idx
        for early_idx in (stop_detect..task_out_start).rev() {
            let other_opt = self.blk_ctx.tasks_manager.task_for_read(early_idx);
            let other = other_opt.as_ref().unwrap();
            // stop loop when we find a colliding peer
            if ExeTask::has_collision(task, other) {
                eei = early_idx;
                break;
            }
        }
        //println!("HERE me={} task_out_start={} eei={}", self.idx, task_out_start, eei);
        eei as i32

        //let my_idx = self.idx;
        //if my_idx < EARLY_EXE_WINDOW_SIZE as i32 {
        //    return FIRST_FRAME;
        //}
        //let eei = my_idx - EARLY_EXE_WINDOW_SIZE as i32;
        //task.set_min_all_done_index(eei as usize);
        //eei
    }

    fn execute(&self) {
        self.blk_ctx.execute_task(self.idx as usize);

        self.get_dashboard().set_executed(self.idx);

        let task_id = self.blk_ctx.dashboard.hi | (self.idx as i64);
        //println!("send_to_ads {:#016x}", task_id);
        self.blk_ctx.send_to_ads(task_id);
    }

    fn get_dashboard(&self) -> &Dashboard {
        &self.blk_ctx.dashboard
    }

    fn get_idx(&self) -> i32 {
        self.idx
    }

    fn get_sibling(&self, idx: i32) -> Self {
        Self {
            idx,
            blk_ctx: self.blk_ctx.clone(),
        }
    }

    fn end_block(&self) {
        self.blk_ctx.end_block();
        self.blk_ctx.code_db.flush().unwrap();
    }
}
