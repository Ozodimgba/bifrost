use std::collections::HashMap;

use solana_client::nonblocking::rpc_client::RpcClient;

use crate::tpu_client::tracker::leader_tracker::RPC_URL;

#[derive(Default, Debug)]
pub struct ScheduleTracker {
    pub curr_epoch_slot_start: u64,
    pub next_epoch_slot_start: u64,
    /// Slot index > leader identity
    pub curr_schedule: HashMap<usize, String>,
    pub next_schedule: HashMap<usize, String>,
    pub slots_in_epoch: u64,
}

impl ScheduleTracker {
    pub async fn new() -> Self {
        let rpc_client = RpcClient::new(RPC_URL.to_string());

        let epoch_info = rpc_client
            .get_epoch_info()
            .await
            .expect("Failed to get epoch info");

        // NOTE: We assume `leader_schedule_slot_offset` is equal to `epoch_info.slots_in_epoch
        // Which is the current case, meaning next leader schedule is calculated at the start of the current epoch.
        // If the above changes, we will have to adjust the calculation.
        let curr_epoch_slot_start = epoch_info.absolute_slot - epoch_info.slot_index;
        let next_epoch_slot_start = curr_epoch_slot_start + epoch_info.slots_in_epoch;

        let curr_schedule =
            Self::get_leader_schedule(&rpc_client, Some(curr_epoch_slot_start)).await;
        let next_schedule =
            Self::get_leader_schedule(&rpc_client, Some(next_epoch_slot_start)).await;

        Self {
            curr_epoch_slot_start,
            next_epoch_slot_start,
            curr_schedule,
            next_schedule,
            slots_in_epoch: epoch_info.slots_in_epoch,
        }
    }

    //TODO: Handle RPC connection failure
    pub async fn get_leader_schedule(
        rpc_client: &RpcClient,
        slot: Option<u64>,
    ) -> HashMap<usize, String> {
        let mut schedule: HashMap<usize, String> = HashMap::new();

        let tmp_schedule = rpc_client
            .get_leader_schedule(slot)
            .await
            .expect("Failed to fetch leader schedule from RPC")
            .expect("No leader schedule for this slot");

        tmp_schedule.iter().for_each(|(val, slots)| {
            slots.iter().for_each(|slot| {
                schedule.insert(*slot, val.clone());
            });
        });
        schedule
    }
}
