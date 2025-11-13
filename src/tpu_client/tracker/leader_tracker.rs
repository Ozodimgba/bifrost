use std::collections::HashMap;
use std::sync::Arc;

use futures_util::stream::StreamExt;
use log::info;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::tpu_client::tracker::schedule_tracking::ScheduleTracker;
use crate::tpu_client::tracker::slots_tracker::SlotsTracker;

pub const RPC_URL: &str = "https://api.testnet.solana.com";
const WS_RPC_URL: &str = "wss://api.testnet.solana.com/";

/**
 * We have 3 actions that are needed in order to track leaders properly:
 * 1. Get current slot
 * 2. Get leader schedule, next_leader_schedule is moved to current on epoch change, and we fetch new next_leader_schedule
 * 3. Update leader ips per identety, because validators might change their IP, so we need to periodically update the ips in the schedule
 *
 * This will allow us to get the leader ip to send TXs to for the next slot, by:
 * 1. Get current slot
 * 2. Get identity of the leader for this current slot (or next slot)
 * 3. Get ip of the leader from the identity
 *
 * The reason we separate identities from IPs and track them separately is because the schedule is based on identities,
 * and the IPs can change anytime, this way we are not locking the schedule while doing ip updates.
**/

#[derive(Default, Debug)]
pub struct LeaderTracker {
    pub slots_tracker: RwLock<SlotsTracker>,
    schedule_tracker: RwLock<ScheduleTracker>,
    leader_sockets: RwLock<HashMap<String, String>>,
}

impl LeaderTracker {
    pub async fn new() -> Self {
        Self {
            slots_tracker: RwLock::new(SlotsTracker::new()),
            schedule_tracker: RwLock::new(ScheduleTracker::new().await),
            leader_sockets: RwLock::new(HashMap::default()),
        }
    }

    pub async fn get_future_leaders(&self, slots_amount: u64) -> Vec<(String, String)> {
        let slot_tracker = self.slots_tracker.read().await;
        let curr_slot = slot_tracker.get_slot();
        drop(slot_tracker);

        if curr_slot == 0 {
            return vec![];
        }

        let mut leaders = vec![];
        let schedule_tracker = self.schedule_tracker.read().await;
        let leader_sockets = self.leader_sockets.read().await;

        // Get current leader and next slot leader if different leader
        for i in 0..slots_amount {
            // Get the index of the slot
            let slot_index = (curr_slot + i - schedule_tracker.curr_epoch_slot_start) as usize;

            // If we have this index in our schedule and not a duplicate add to leaders vector
            if let Some(leader) = schedule_tracker.curr_schedule.get(&slot_index)
                && let Some(leader_socket) = leader_sockets.get(leader)
                && !leaders.contains(&(leader.to_string(), leader_socket.to_string()))
            {
                leaders.push((leader.clone(), leader_socket.clone()));
            }
        }

        leaders
    }

    /// Get the current and next leaders
    /// Output = (leader identity, leader socket)
    pub async fn get_leaders(&self) -> Vec<(String, String)> {
        self.get_future_leaders(2).await
    }

    /// Get all cluster node leader IPs
    /// TODO: We want to do period ip updates for the schedules to keep ips up to date
    /// and not try to connect to a hotswap node or something
    pub async fn update_leader_sockets(leader_tracker: Arc<LeaderTracker>) {
        let mut leader_sockets: HashMap<String, String> = HashMap::new();
        let rpc_client = RpcClient::new(RPC_URL.to_string());

        let nodes = rpc_client
            .get_cluster_nodes()
            .await
            .expect("Failed to get cluster nodes");

        nodes.iter().for_each(|node| {
            if let Some(tpu_quic) = node.tpu_quic {
                leader_sockets.insert(node.pubkey.to_string(), tpu_quic.to_string());
            }
        });

        let mut ls = leader_tracker.leader_sockets.write().await;
        *ls = leader_sockets;
    }

    /// Run the slot updates listener
    pub async fn run(leader_tracker: Arc<LeaderTracker>) {
        let ws_client = PubsubClient::new(WS_RPC_URL).await.unwrap();

        let (mut slot_notifications, _unsubscribe) =
            ws_client.slot_updates_subscribe().await.unwrap();
        info!("Listening for slot updates...\n");

        while let Some(slot_event) = slot_notifications.next().await {
            let mut slot_tracker_lock = leader_tracker.slots_tracker.write().await;

            // Update slot tracker
            if let None = slot_tracker_lock.record(slot_event) {
                continue;
            }

            let curr_slot = slot_tracker_lock.get_slot();
            drop(slot_tracker_lock);

            // If slot is from next epoch, update schedule
            let mut schedule_tracker = leader_tracker.schedule_tracker.write().await;

            if curr_slot >= schedule_tracker.next_epoch_slot_start {
                // curr epoch slot start
                schedule_tracker.curr_epoch_slot_start = schedule_tracker.next_epoch_slot_start;

                // Set next epoch slot start
                schedule_tracker.next_epoch_slot_start =
                    schedule_tracker.next_epoch_slot_start + schedule_tracker.slots_in_epoch;

                // set curr schedule to next schedule
                schedule_tracker.curr_schedule =
                    std::mem::take(&mut schedule_tracker.next_schedule);

                let next_epoch_slot_start = schedule_tracker.next_epoch_slot_start;
                drop(schedule_tracker);

                // we asynchronously update the next schedule with minimal locking
                // because we have the whole epoch to update it.
                let leader_tracker_clone = leader_tracker.clone();
                tokio::spawn(async move {
                    let rpc_client = RpcClient::new(RPC_URL.to_string());

                    let next_schedule = ScheduleTracker::get_leader_schedule(
                        &rpc_client,
                        Some(next_epoch_slot_start),
                    )
                    .await;
                    let mut schedule_tracker = leader_tracker_clone.schedule_tracker.write().await;
                    schedule_tracker.next_schedule = next_schedule;
                });
            }
        }

        // TODO: Handle closing the subscription properly
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_get_rpc_leader_schedule() {
        let leader_tracker: Arc<LeaderTracker> = Arc::new(LeaderTracker::new().await);

        let leader_tracker_clone = leader_tracker.clone();
        tokio::spawn(async move { LeaderTracker::run(leader_tracker_clone).await });

        let leader_tracker_clone = leader_tracker.clone();
        tokio::spawn(async move {
            LeaderTracker::update_leader_sockets(leader_tracker_clone).await;

            sleep(Duration::from_secs(60)).await;
        });

        loop {
            sleep(Duration::from_millis(400)).await;

            let lt = leader_tracker.slots_tracker.read().await;
            let curr_slot = lt.get_slot();
            if curr_slot == 0 {
                continue;
            }
            drop(lt);

            println!(
                "Current Slot: {},leaders: {:?}",
                curr_slot,
                leader_tracker.get_leaders().await
            );
        }
    }
}
