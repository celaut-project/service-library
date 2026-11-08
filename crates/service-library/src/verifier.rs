//! An optional verifier of what the node says about the instances of a service: the signed
//! balance statements of an instance, and of its children and grandchildren (nodo#631).
//!
//! It answers the questions that a service has when its dependency goes away:
//!
//! - Did the node stop the instance, and why? An instance that the service did not stop,
//!   and whose account ends with a `stop`, was stopped by someone else: the node, the
//!   balance (`out_of_balance`) or an ancestor ([`Finding::StoppedWithoutUs`]).
//! - Did the instance die with MU that nobody got? ([`Finding::StoppedWithMu`], the fault
//!   that nodo scores as `killed_with_mu`.)
//! - Is the refund that `StopService` gave back in the signed ledger? ([`Finding::RefundMismatch`].)
//! - Are the children and grandchildren that the instance started alive, and did the node
//!   keep to the rules with them? The children are the `child_bound` movements of the
//!   account of the instance, and the grandchildren those of each child.
//! - Is the statement itself good? The signature must be the one of the node, the hash
//!   chain must hold, the head must not go back and the balance must add up
//!   ([`crate::statement::verify`]). A node that gives a statement that fails is
//!   [`Finding::InvalidStatement`], and the node did charge the read.
//!
//! # Cost, and why it is opt-in
//!
//! A read of a statement costs MU and writes a `statement_query` movement on the node. So
//! the verifier never reads unless the service built it, and it has a **budget**
//! ([`VerifierConfig::budget_mu`]) that is its whole spending: before each read it asks what
//! the read can cost at most ([`StatementRequest::max_mu_needed`]), refuses the read if that
//! is more than what is left ([`Finding::BudgetExhausted`]), and takes that amount off the
//! budget (the real charge is never more). A budget of 0 reads nothing. It also asks the
//! node for a bounded number of rows, so a read is cheap and an account is read once: the
//! next pass goes on from the last row that it saw.
//!
//! # What it cannot do
//!
//! A statement proves what the node signed, not that the node is honest: a node can refuse
//! to show one ([`Finding::StatementRefused`], which can hide a death with MU kept), and it
//! can delete old movements (its retention, [`InstanceReport::gap`]). The identity of the
//! node is the `peer_id` that the service pinned ([`crate::gateway::Gateway::with_peer_id`]);
//! without it the verifier takes the `public_key` that the node gives about itself, which
//! proves that the statements are consistent with that key and nothing more
//! ([`Verifier::identity_pinned`]).

use std::collections::HashMap;

use serde::Serialize;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::gateway::Gateway;
use crate::instance::token_tag;
use crate::statement::{self, KeptHead, Kind, Rates, StatementError, StatementRequest, Stop};
use crate::Error;

/// The factor that bounds the `tick` of a `stop`, as nodo does (`_MAX_TICK_FACTOR`).
const MAX_TICK_FACTOR: u128 = 10;

/// How long after a stop that the service asked for the ledger must show it before the
/// verifier says that the account is not stopped ([`Finding::NotStopped`]), in seconds.
pub const STOP_GRACE_SECONDS: u64 = 120;

/// What the verifier may do. It starts from [`VerifierConfig::with_budget`].
#[derive(Debug, Clone)]
pub struct VerifierConfig {
    /// The most MU that the verifier may spend on reads, in all its life. 0 reads nothing.
    pub budget_mu: u128,
    /// How deep it goes below an instance: 1 reads the children, 2 the grandchildren too.
    pub max_depth: u32,
    /// The most children that it reads under one instance.
    pub max_children: usize,
    /// The most movements that it asks for in one read.
    pub rows_per_read: u64,
    /// The most reads for one account in one pass.
    pub max_pages: u32,
    /// The most `remaining_hops` that it gives to a read of a descendant. A read of a
    /// descendant at depth `d` gets `d + 1` hops, at least 2 and at most this.
    pub max_hops: u32,
}

impl VerifierConfig {
    /// The defaults, with a budget of `budget_mu` MU: children and grandchildren, 100 rows
    /// per read, 5 reads per account and 64 children per instance.
    pub fn with_budget(budget_mu: u128) -> Self {
        VerifierConfig {
            budget_mu,
            max_depth: 2,
            max_children: 64,
            rows_per_read: 100,
            max_pages: 5,
            max_hops: 4,
        }
    }
}

/// What the service knows about how an instance ended, to compare with the ledger.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Expectation {
    /// The service stopped the instance (a stop that the node answered).
    pub stopped_by_us: bool,
    /// The MU that the node said it gave back at that stop.
    pub refund_claimed_mu: Option<u128>,
    /// The seconds since the service stopped it.
    pub stopped_secs_ago: u64,
}

/// Why a node refused to show a statement (the status of the call).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalKind {
    /// `PERMISSION_DENIED`: no chain below the token links the descendant.
    NotLinked,
    /// `RESOURCE_EXHAUSTED`: the caller or an instance cannot pay.
    CannotPay,
    /// `UNAVAILABLE`: the descendant or a node below does not answer.
    Unavailable,
    /// `NOT_FOUND`.
    NotFound,
    /// `UNIMPLEMENTED`: a delegated instance.
    Delegated,
    /// Another text.
    Other,
}

impl RefusalKind {
    /// The kind of the text of a failed call. The text of the call holds the gRPC status as
    /// its name (`PermissionDenied`) or as the sentence that the transport writes for it
    /// ("The caller does not have permission ..."), so both are looked for.
    pub fn of_text(text: &str) -> RefusalKind {
        let t: String = text
            .chars()
            .filter(|c| c.is_ascii_alphabetic())
            .collect::<String>()
            .to_ascii_lowercase();
        let has = |names: &[&str]| names.iter().any(|n| t.contains(n));
        if has(&["permissiondenied", "doesnothavepermission"]) {
            RefusalKind::NotLinked
        } else if has(&["resourceexhausted", "resourcehasbeenexhausted"]) {
            RefusalKind::CannotPay
        } else if has(&["unavailable"]) {
            RefusalKind::Unavailable
        } else if has(&["notfound", "entitywasnotfound"]) {
            RefusalKind::NotFound
        } else if has(&["unimplemented", "isnotimplemented"]) {
            RefusalKind::Delegated
        } else {
            RefusalKind::Other
        }
    }
}

/// Something that the service should know about.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Finding {
    /// The account ended with MU above 0, the father was gone, and nobody got the MU (the
    /// fault that nodo scores as `killed_with_mu`). A stop whose `refund` went to a live
    /// father is not one.
    StoppedWithMu {
        instance_ref: String,
        reason: String,
        leftover_mu: u128,
        father_gone: bool,
    },
    /// The account of an instance that the service did not stop ends with a `stop`: it was
    /// stopped by the node, by its own balance (`out_of_balance`) or by an ancestor.
    StoppedWithoutUs {
        instance_ref: String,
        reason: String,
    },
    /// The service stopped the instance, and the ledger gives another reason for the stop
    /// (`manual_kill` is the one of a stop that the father asks for).
    UnexpectedStopReason {
        instance_ref: String,
        reason: String,
    },
    /// The service stopped the instance, the node said it gave back `claimed_mu`, and the
    /// signed ledger gives back less.
    RefundMismatch {
        instance_ref: String,
        claimed_mu: u128,
        ledger_mu: u128,
    },
    /// The service stopped the instance more than [`STOP_GRACE_SECONDS`] ago and the ledger,
    /// read up to its head, has no `stop`.
    NotStopped { instance_ref: String },
    /// The node refused to show the statement. If the instance died with MU kept, the
    /// verifier cannot see it.
    StatementRefused {
        instance_ref: String,
        refusal: RefusalKind,
        text: String,
    },
    /// The statement did not pass the checks (signature, chain, head, date, balance). The
    /// node did charge the read.
    InvalidStatement { instance_ref: String, text: String },
    /// The node does not know the `instance_ref` of nodo#631. The read was charged.
    NoDescendants { instance_ref: String },
    /// A statement named another service for the instance than the first one that the
    /// verifier kept.
    ServiceChanged { instance_ref: String },
    /// The budget of the verifier is not enough for the next read, so the account (and what
    /// is under it) was not read.
    BudgetExhausted {
        instance_ref: String,
        needed_mu: u128,
    },
    /// The node says that it deleted movements before the first one that it gave. A child
    /// can be in them.
    Gap { instance_ref: String },
    /// The pass stopped before the head: the rows that are not read can hold a `stop`.
    Incomplete { instance_ref: String },
    /// Children that were left out because they are deeper than the configured depth.
    TooDeep {
        instance_ref: String,
        children: usize,
    },
    /// Children that were left out because the instance has more than the configured number.
    TooMany {
        instance_ref: String,
        children: usize,
    },
}

/// Where an account is, as far as the last read shows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AccountState {
    /// A `stop` movement ended the account. Nothing follows a `stop`.
    Stopped { reason: String, leftover_mu: u128 },
    /// The statement, read up to its head, has no `stop`.
    Alive,
    /// The read did not reach the head, or it did not happen.
    Unknown,
}

/// What one pass learned about an account and what is below it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstanceReport {
    /// The `instance_ref` of the account, 64 lowercase hex characters.
    pub instance_ref: String,
    /// 0 for the instance of the service, 1 for its children, 2 for their children.
    pub depth: u32,
    /// The service of the instance, in lowercase hex, if the node said it.
    pub service_id: Option<String>,
    pub state: AccountState,
    /// The signed balance at the head, in MU, if a read reached it.
    pub balance_mu: Option<i128>,
    /// The `seq` of the last movement that the verifier has seen.
    pub head_seq: u64,
    /// True if the node says that it deleted movements.
    pub gap: bool,
    /// The children that the account links.
    pub children: Vec<InstanceReport>,
    /// What the verifier found, for this account only.
    pub findings: Vec<Finding>,
}

impl InstanceReport {
    /// Every finding of this account and the ones below it.
    pub fn all_findings(&self) -> Vec<&Finding> {
        let mut out: Vec<&Finding> = self.findings.iter().collect();
        for child in &self.children {
            out.extend(child.all_findings());
        }
        out
    }
}

/// Why the verifier cannot be built.
#[derive(Debug, thiserror::Error)]
pub enum VerifierError {
    /// The call to the node failed.
    #[error(transparent)]
    Node(#[from] Error),
    /// The node does not advertise `statement_query_mu` and `statement_row_mu`, so the
    /// verifier cannot know what a read costs, and it does not read.
    #[error("the node does not advertise the rates of a statement read")]
    NoRates,
    /// The node does not give a `public_key` and none was pinned.
    #[error("the node gave no public key and no peer_id was pinned")]
    NoIdentity,
    /// The `peer_id` that the service pinned is not the key that the node advertises.
    #[error("the node advertises the key {advertised}, and the pinned peer_id is {pinned}")]
    IdentityMismatch { pinned: String, advertised: String },
}

/// What the verifier keeps of an account between two passes.
#[derive(Default, Clone)]
struct Account {
    rows_seq: u64,
    head: Option<KeptHead>,
    service_id: Option<String>,
    children: Vec<(String, Option<String>)>,
    stop: Option<Stop>,
    father_gone: bool,
    refund_mu: u128,
    /// The largest maintenance charge seen: it bounds the `tick` of an `out_of_balance` stop.
    period_mu: u128,
    balance: Option<i128>,
    gap: bool,
    complete: bool,
}

#[derive(Default)]
struct State {
    spent: u128,
    accounts: HashMap<String, Account>,
}

/// The verifier. It is cheap to share: its state is behind a lock.
pub struct Verifier {
    gateway: Gateway,
    peer_id: String,
    pinned: bool,
    rates: Rates,
    config: VerifierConfig,
    state: Mutex<State>,
}

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier")
            .field("peer_id", &self.peer_id)
            .field("identity_pinned", &self.pinned)
            .field("rates", &self.rates)
            .field("config", &self.config)
            .finish()
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Verifier {
    /// A verifier on `gateway`. It asks the node for its rates and its public key (free).
    /// If the gateway has a pinned `peer_id` ([`Gateway::with_peer_id`]) the node must
    /// advertise that key; else the verifier takes the key that the node advertises.
    pub async fn connect(gateway: Gateway, config: VerifierConfig) -> Result<Self, VerifierError> {
        let peer = gateway.peer_info().await?;
        let rates = Rates::from_peer(&peer).ok_or(VerifierError::NoRates)?;
        let (peer_id, pinned) = match gateway.peer_id() {
            Some(pinned) => {
                if !peer.public_key.is_empty() && peer.public_key != pinned {
                    return Err(VerifierError::IdentityMismatch {
                        pinned: pinned.to_string(),
                        advertised: peer.public_key,
                    });
                }
                (pinned.to_string(), true)
            }
            None if peer.public_key.is_empty() => return Err(VerifierError::NoIdentity),
            None => (peer.public_key, false),
        };
        Ok(Verifier {
            gateway,
            peer_id,
            pinned,
            rates,
            config,
            state: Mutex::new(State::default()),
        })
    }

    /// True if the service gave the `peer_id` of its node. If not, the statements are
    /// checked against the key that the node advertised about itself.
    pub fn identity_pinned(&self) -> bool {
        self.pinned
    }

    /// The MU spent on reads (the most that they could cost), and the MU left.
    pub fn budget(&self) -> (u128, u128) {
        let spent = self.state.lock().unwrap().spent;
        (spent, self.config.budget_mu.saturating_sub(spent))
    }

    /// Read what the node signed about the instance with `token`, and about what is below
    /// it (up to `max_depth`), and compare it with what the service knows (`expected`).
    /// This is the only call that reads, and it reads at most what the budget allows.
    pub async fn check(&self, token: &str, expected: Expectation) -> InstanceReport {
        let root_ref = statement::instance_ref(token);
        let mut visited = vec![root_ref.clone()];
        let mut report = self
            .check_account(token, None, &root_ref, 0, 1, expected, &mut visited)
            .await;
        report.depth = 0;
        report
    }

    #[allow(clippy::too_many_arguments)]
    async fn check_account(
        &self,
        token: &str,
        target: Option<&str>,
        account_ref: &str,
        depth: u32,
        hops: u32,
        expected: Expectation,
        visited: &mut Vec<String>,
    ) -> InstanceReport {
        let key = format!("{}/{}", statement::instance_ref(token), account_ref);
        let mut findings = Vec::new();
        let read = self
            .read_account(token, target, account_ref, &key, hops, &mut findings)
            .await;
        let account = self
            .state
            .lock()
            .unwrap()
            .accounts
            .get(&key)
            .cloned()
            .unwrap_or_default();

        if let Some(stop) = &account.stop {
            if depth == 0 {
                self.judge_own_stop(account_ref, &account, stop, expected, &mut findings);
            }
            if stop.leftover > 0 && stop_is_a_fault(&account, stop) {
                findings.push(Finding::StoppedWithMu {
                    instance_ref: account_ref.to_string(),
                    reason: stop.reason.clone(),
                    leftover_mu: stop.leftover,
                    father_gone: account.father_gone,
                });
            }
        } else if depth == 0
            && expected.stopped_by_us
            && account.complete
            && expected.stopped_secs_ago > STOP_GRACE_SECONDS
        {
            findings.push(Finding::NotStopped {
                instance_ref: account_ref.to_string(),
            });
        }
        if account.gap {
            findings.push(Finding::Gap {
                instance_ref: account_ref.to_string(),
            });
        }
        if read && !account.complete && account.stop.is_none() {
            findings.push(Finding::Incomplete {
                instance_ref: account_ref.to_string(),
            });
        }

        let state = match (&account.stop, account.complete) {
            (Some(stop), _) => AccountState::Stopped {
                reason: stop.reason.clone(),
                leftover_mu: stop.leftover,
            },
            (None, true) => AccountState::Alive,
            (None, false) => AccountState::Unknown,
        };

        let mut children = Vec::new();
        if read && depth < self.config.max_depth {
            let linked = account.children.len();
            let wanted = account.children.iter().take(self.config.max_children);
            for (child_ref, _) in wanted {
                if visited.contains(child_ref) {
                    continue;
                }
                visited.push(child_ref.clone());
                let child_hops = (depth + 2).clamp(2, self.config.max_hops.max(2));
                let child = Box::pin(self.check_account(
                    token,
                    Some(child_ref),
                    child_ref,
                    depth + 1,
                    child_hops,
                    Expectation::default(),
                    visited,
                ))
                .await;
                children.push(child);
            }
            if linked > self.config.max_children {
                findings.push(Finding::TooMany {
                    instance_ref: account_ref.to_string(),
                    children: linked - self.config.max_children,
                });
            }
        } else if read && !account.children.is_empty() {
            findings.push(Finding::TooDeep {
                instance_ref: account_ref.to_string(),
                children: account.children.len(),
            });
        }

        InstanceReport {
            instance_ref: account_ref.to_string(),
            depth,
            service_id: account.service_id.clone(),
            state,
            balance_mu: account.balance,
            head_seq: account.rows_seq,
            gap: account.gap,
            children,
            findings,
        }
    }

    /// What the ledger says about an instance that the service holds, against what the
    /// service did.
    fn judge_own_stop(
        &self,
        account_ref: &str,
        account: &Account,
        stop: &Stop,
        expected: Expectation,
        findings: &mut Vec<Finding>,
    ) {
        if !expected.stopped_by_us {
            findings.push(Finding::StoppedWithoutUs {
                instance_ref: account_ref.to_string(),
                reason: stop.reason.clone(),
            });
            return;
        }
        if stop.reason != "manual_kill" {
            findings.push(Finding::UnexpectedStopReason {
                instance_ref: account_ref.to_string(),
                reason: stop.reason.clone(),
            });
        }
        if let Some(claimed) = expected.refund_claimed_mu {
            if account.refund_mu < claimed {
                findings.push(Finding::RefundMismatch {
                    instance_ref: account_ref.to_string(),
                    claimed_mu: claimed,
                    ledger_mu: account.refund_mu,
                });
            }
        }
    }

    /// Read the new movements of one account, page by page, within the budget. It keeps
    /// the state and gives `false` if it could not read at all (a finding says why).
    async fn read_account(
        &self,
        token: &str,
        target: Option<&str>,
        account_ref: &str,
        key: &str,
        hops: u32,
        findings: &mut Vec<Finding>,
    ) -> bool {
        let mut pages = 0;
        loop {
            let (mut account, from_seq) = {
                let state = self.state.lock().unwrap();
                let account = state.accounts.get(key).cloned().unwrap_or_default();
                if account.stop.is_some() {
                    // Nothing follows a `stop`: it is not read again.
                    return true;
                }
                let from = account.rows_seq + 1;
                (account, from)
            };
            if pages >= self.config.max_pages {
                return true;
            }
            pages += 1;
            let request = StatementRequest {
                token: token.to_string(),
                instance_ref: target.map(str::to_string).unwrap_or_default(),
                from_seq,
                to_seq: from_seq + self.config.rows_per_read.max(1) - 1,
                remaining_hops: hops,
            };
            let needed = request.max_mu_needed(&self.rates);
            {
                let mut state = self.state.lock().unwrap();
                let left = self.config.budget_mu.saturating_sub(state.spent);
                if needed > left {
                    findings.push(Finding::BudgetExhausted {
                        instance_ref: account_ref.to_string(),
                        needed_mu: needed,
                    });
                    return false;
                }
                // The charge is never more than `needed`, so this is a safe count.
                state.spent += needed;
            }
            let answer = self
                .gateway
                .balance_statement(
                    &request,
                    &self.peer_id,
                    &self.rates,
                    needed,
                    account.head.as_ref(),
                    now(),
                )
                .await;
            let verified = match answer {
                Ok(v) => v,
                Err(StatementError::NoDescendants) => {
                    findings.push(Finding::NoDescendants {
                        instance_ref: account_ref.to_string(),
                    });
                    return false;
                }
                Err(StatementError::Invalid(text)) => {
                    findings.push(Finding::InvalidStatement {
                        instance_ref: account_ref.to_string(),
                        text,
                    });
                    return false;
                }
                Err(StatementError::Wire(error)) => {
                    // A call that did not go out is not charged: give the MU back.
                    if matches!(
                        error,
                        Error::Gateway {
                            unreachable: true,
                            ..
                        }
                    ) {
                        let mut state = self.state.lock().unwrap();
                        state.spent = state.spent.saturating_sub(needed);
                    }
                    let text = error.to_string();
                    findings.push(Finding::StatementRefused {
                        instance_ref: account_ref.to_string(),
                        refusal: RefusalKind::of_text(&text),
                        text,
                    });
                    return false;
                }
                Err(other) => {
                    findings.push(Finding::InvalidStatement {
                        instance_ref: account_ref.to_string(),
                        text: other.to_string(),
                    });
                    return false;
                }
            };

            if !verified.service_id.is_empty() {
                match account.service_id.as_deref() {
                    None => account.service_id = Some(verified.service_id.clone()),
                    Some(known) if known != verified.service_id => {
                        // The first service that a node gave stays: nothing of this answer is kept.
                        findings.push(Finding::ServiceChanged {
                            instance_ref: account_ref.to_string(),
                        });
                        return false;
                    }
                    Some(_) => {}
                }
            }
            if let Some(last) = verified.movements.last() {
                account.rows_seq = account.rows_seq.max(last.seq);
            }
            account.head = Some(verified.head.clone());
            account.balance = Some(verified.balance);
            account.gap |= verified.gap;
            for (child_ref, bound) in verified.children() {
                if !account.children.iter().any(|(r, _)| r == child_ref) {
                    account
                        .children
                        .push((child_ref.to_string(), bound.service_id.clone()));
                }
            }
            for movement in &verified.movements {
                if movement.kind == Kind::Maintenance && movement.amount < 0 {
                    account.period_mu = account.period_mu.max(movement.amount.unsigned_abs());
                }
                if movement.kind == Kind::Refund {
                    // A sum: a stop can write more than one `refund` row, and a sum never
                    // accuses a node of a refund that it did give.
                    account.refund_mu = account
                        .refund_mu
                        .saturating_add(movement.amount.unsigned_abs());
                    if lost_father_gone(&movement.detail) {
                        account.father_gone = true;
                    }
                }
            }
            if let Some(stop) = verified.stop() {
                account.stop = Some(stop.clone());
            }
            let more = verified.next_from_seq.is_some();
            account.complete = !more;
            let stopped = account.stop.is_some();
            self.state
                .lock()
                .unwrap()
                .accounts
                .insert(key.to_string(), account);
            if !more || stopped {
                return true;
            }
        }
    }
}

fn lost_father_gone(detail: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(detail)
        .ok()
        .and_then(|d| {
            d.get("lost")
                .and_then(|v| v.as_str())
                .map(|s| s == "father_gone")
        })
        .unwrap_or(false)
}

/// True if a `stop` with a leftover above 0 is the fault that nodo scores as
/// `killed_with_mu` (`balance_statement._judge_stop`): the MU stayed with the node because
/// the father was gone. A father that stopped its child got the MU, so that stop is not a
/// fault. An `out_of_balance` with less left than the tick that the instance could not pay
/// is not one either.
fn stop_is_a_fault(account: &Account, stop: &Stop) -> bool {
    if !account.father_gone {
        return false;
    }
    if stop.reason != "out_of_balance" {
        return true;
    }
    // The tick comes from the row, bounded by 10 times the largest maintenance row; a row
    // with no tick uses that row, and with no maintenance row the stop is not judged.
    let period = account.period_mu;
    let tick = match stop.tick.as_deref().and_then(|t| t.parse::<u128>().ok()) {
        Some(t) if period > 0 => t.min(period.saturating_mul(MAX_TICK_FACTOR)),
        Some(t) => t,
        None => period,
    };
    tick != 0 && stop.leftover >= tick
}

/// The tag of a token, for a log line: never the token.
pub fn tag(token: &str) -> String {
    token_tag(token)
}
