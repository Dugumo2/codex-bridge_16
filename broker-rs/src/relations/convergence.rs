//! A 路收敛事实入口；缺失上下文不能当作空树。

use crate::claim::Machine;
use crate::sa1b::ConvergenceFacts;

impl Machine {
    pub fn convergence_facts(&self, _run_id: &str) -> Option<ConvergenceFacts> {
        None
    }
}
