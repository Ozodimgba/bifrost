
//! Leader Schedule Construction Without RPC
//!
//! This module should implement Solana's leader schedule algorithm locally, computing the same
//! deterministic schedule that all validators use without calling `getLeaderSchedule` RPC.
//!
//! To build the leader schedule, we need:
//!
//! 1. **Stake Data** - Map of `Pubkey → u64` stake amounts for each validator
//!    - Fetch once per epoch via `getVoteAccounts` RPC and cache
//!    - Alternative: Parse stake account state from ledger/shreds
//!
//! 2. **Epoch Number** - The epoch for which to compute the schedule
//!    - Calculate from: current_slot / slots_per_epoch
//!
//! 3. **Slots Per Epoch** - Total slots in the epoch (e.g., 432,000 on mainnet)
//!    - Fetch once via `getEpochSchedule` RPC and cache
//!    - Or use known constant for your cluster
//!
//! Once we have this data, compute the schedule locally and cache it for the entire epoch.
//!
//! Core Algorithm: `stake_weighted_slot_leaders()`
//!
//! The Agave algorithm from `agave/ledger/src/leader_schedule.rs`:
//!
//! ```rust,ignore
//! use rand::distributions::{Distribution, WeightedIndex};
//! use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
//!
//! fn stake_weighted_slot_leaders(
//!     mut keyed_stakes: Vec<(&Pubkey, u64)>,
//!     epoch: u64,
//!     len: u64,      // slots_per_epoch
//!     repeat: u64,   // NUM_CONSECUTIVE_LEADER_SLOTS = 4
//! ) -> Vec<Pubkey> {
//!     // Step 1: Deterministic sorting
//!     sort_stakes(&mut keyed_stakes);
//!     
//!     // Step 2: Separate keys and weights
//!     let (keys, stakes): (Vec<_>, Vec<_>) = keyed_stakes.into_iter().unzip();
//!     
//!     // Step 3: Create weighted distribution
//!     let weighted_index = WeightedIndex::new(stakes).unwrap();
//!     
//!     // Step 4: Seed deterministic RNG with epoch
//!     let mut seed = [0u8; 32];
//!     seed[0..8].copy_from_slice(&epoch.to_le_bytes());
//!     let rng = &mut ChaChaRng::from_seed(seed);
//!     
//!     // Step 5: Generate schedule with leader chunks
//!     let mut current_slot_leader = Pubkey::default();
//!     (0..len)
//!         .map(|i| {
//!             if i % repeat == 0 {
//!                 current_slot_leader = keys[weighted_index.sample(rng)];
//!             }
//!             current_slot_leader
//!         })
//!         .collect()
//! }
//!
//! fn sort_stakes(stakes: &mut Vec<(&Pubkey, u64)>) {
//!     // Sort by stake DESC, then pubkey DESC for determinism
//!     stakes.sort_unstable_by(|(l_pubkey, l_stake), (r_pubkey, r_stake)| {
//!         if r_stake == l_stake {
//!             r_pubkey.cmp(l_pubkey)  // Tie-breaker
//!         } else {
//!             r_stake.cmp(l_stake)
//!         }
//!     });
//!     stakes.dedup();
//! }
//! ```
//!
//! Algorithm Breakdown
//!
//! i: Deterministic Sorting
//!
//! Validators must be sorted identically on all nodes:
//! - **Primary sort**: By stake amount (highest first)
//! - **Tie-breaker**: By pubkey (reverse lexicographic order)
//! - **Deduplication**: Remove any duplicate entries
//!
//! ```rust,ignore
//! // Example stakes before sorting:
//! [
//!     (Pubkey("AAA..."), 1000),
//!     (Pubkey("BBB..."), 1000),  // Same stake
//!     (Pubkey("CCC..."), 500),
//! ]
//!
//! // After sorting:
//! [
//!     (Pubkey("BBB..."), 1000),  // Higher pubkey wins tie
//!     (Pubkey("AAA..."), 1000),
//!     (Pubkey("CCC..."), 500),
//! ]
//! ```
//!
//! ii: Weighted Distribution
//!
//! Use `rand::distributions::WeightedIndex` to create a probability distribution:
//! - Validators with more stake get selected more frequently
//! - Probability of selection = validator_stake / total_stake
//!
//! iii: Deterministic RNG
//!
//! Seed ChaCha20 RNG with the epoch number:
//! - `seed[0..8] = epoch.to_le_bytes()`
//! - All nodes use identical seed → identical random sequence
//! - Schedule is deterministic and verifiable
//!
//! Step iv: 4-Slot Leader Chunks
//!
//! Leaders are assigned in groups of 4 consecutive slots:
//! - Slot 0-3: Leader A
//! - Slot 4-7: Leader B
//! - Slot 8-11: Leader C
//! - etc.
//!
//! Every 4th slot (when `i % 4 == 0`), sample a new leader from the weighted distribution.
