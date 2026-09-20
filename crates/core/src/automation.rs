//! Automations (#220): scheduled and webhook-triggered spawns.
//!
//! Two layers. The **rules** — cron evaluation, webhook signatures,
//! task-template interpolation — are pure functions over borrowed data, with
//! `now` supplied by the caller, so every one of them is testable without
//! waiting for wall-clock time to pass. [`AutomationEngine`] is the thin
//! stateful shell that reads the store, decides what is due, and spawns.

use std::str::FromStr as _;

use chrono::{DateTime, SecondsFormat, Utc};

use crate::error::{CoreError, Result};

pub use prospero_types::{
    Automation, AutomationRun, CreateAutomationBody, CreatedAutomationResponse, FiredResponse,
    RunSource, SetEnabledBody, SpawnTemplate, Trigger,
};

/// An automation as persisted, including the secret the wire type omits.
///
/// [`Automation`] is shared with the dashboard and every API response;
/// `webhook_secret` deliberately lives only here, so there is no path by which
/// serializing an automation exposes its signing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAutomation {
    /// The automation itself — the part that is safe to hand out.
    pub automation: Automation,
    /// HMAC signing key for a webhook trigger. Shown once, at creation.
    pub webhook_secret: Option<String>,
}

/// Runs retained per automation. Run history is a debugging aid, not an audit
/// log (the event store holds those), so it is bounded rather than growing
/// without limit in the config DB.
pub const RUN_HISTORY_LIMIT: usize = 200;

/// Render a timestamp the one way the store compares them.
///
/// Fire times are compared **lexicographically** by the claim statement's
/// `WHERE last_fired_at < ?`, which is only equivalent to chronological order
/// if every timestamp has the same fixed width and zone. Whole seconds and a
/// literal `Z` guarantee that; a stray `+00:00` or fractional-second stamp
/// would silently break the ordering that keeps a tick from firing twice.
pub fn stamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Adapt an operator-written cron expression to the 6-field form the `cron`
/// crate parses.
///
/// Operators write standard 5-field cron (`min hour dom mon dow`) — that is
/// what crontab, Kubernetes `CronJob` and every scheduling UI mean by "cron".
/// The `cron` crate instead expects a leading **seconds** field, so a plain
/// `*/5 * * * *` would silently parse as *every 5 seconds* rather than every 5
/// minutes. Prepending `0` pins such a schedule to the top of its minute.
///
/// A 6- or 7-field expression is passed through untouched, so the crate's own
/// seconds/year syntax stays available to anyone who wants it.
pub fn normalize_schedule(schedule: &str) -> String {
    let trimmed = schedule.trim();
    if trimmed.split_whitespace().count() == 5 {
        format!("0 {trimmed}")
    } else {
        trimmed.to_string()
    }
}

/// Parse an operator-written schedule, normalizing the 5-field form first.
///
/// Returned as a `String` error so the API layer can hand the operator the
/// reason their expression was rejected at create time, rather than the
/// automation silently never firing.
pub fn parse_schedule(schedule: &str) -> std::result::Result<cron::Schedule, String> {
    cron::Schedule::from_str(&normalize_schedule(schedule))
        .map_err(|e| format!("invalid cron schedule '{schedule}': {e}"))
}

/// The scheduled tick this automation owes, or `None` if it owes nothing yet.
///
/// `since` is the anchor: the last fire if there was one, otherwise the
/// automation's creation time — so a newly created automation waits for its
/// next scheduled tick instead of firing the instant it is saved.
///
/// **Missed ticks coalesce.** If the daemon was down across several ticks this
/// returns only the most recent one, so an hour of downtime on a `*/5` schedule
/// produces a single catch-up run rather than twelve simultaneous agents.
pub fn due_fire(schedule: &str, since: DateTime<Utc>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let parsed = parse_schedule(schedule).ok()?;
    // Walk *backwards* from now and take the first tick. Walking forwards from
    // `since` instead would cost one iteration per elapsed tick, so a long
    // outage on a per-minute schedule would spin through the whole gap on
    // every poll; this is one step whatever the gap.
    parsed.after(&now).next_back().filter(|tick| *tick > since)
}

/// Longest substituted value, in bytes. A webhook payload is caller-controlled,
/// and an unbounded field would let one request push a multi-megabyte prompt
/// into an agent. Longer values are truncated with an explicit marker rather
/// than silently cut, so the agent can see that it is reading a fragment.
pub const MAX_SUBSTITUTION: usize = 4096;

/// Substitute `{{ dotted.path }}` placeholders in a task template from `payload`.
///
/// **The payload is data, not instruction.** Whoever writes the automation
/// chooses where a webhook's fields land in the prompt; the sender only fills
/// those slots. Values are truncated at [`MAX_SUBSTITUTION`].
///
/// A path that is missing or `null` is an error naming that path — the run is
/// recorded as failed instead of spawning an agent on a half-formed prompt.
pub fn interpolate(
    template: &str,
    payload: &serde_json::Value,
) -> std::result::Result<String, String> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        // An unclosed `{{` is a template typo, not a substitution: emit the
        // remainder verbatim so the operator sees their own text back.
        let Some(end) = after.find("}}") else {
            out.push_str(&rest[start..]);
            return Ok(out);
        };
        out.push_str(&lookup(payload, after[..end].trim())?);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Resolve one dotted path against the payload, rendered for a prompt.
///
/// Object keys only — a path segment is never read as an array index, so a
/// template cannot reach into a list the automation's author did not name.
fn lookup(payload: &serde_json::Value, path: &str) -> std::result::Result<String, String> {
    let mut cursor = payload;
    for segment in path.split('.') {
        cursor = cursor
            .as_object()
            .and_then(|map| map.get(segment))
            .ok_or_else(|| format!("task template references '{path}', absent from the payload"))?;
    }
    let rendered = match cursor {
        serde_json::Value::Null => {
            return Err(format!(
                "task template references '{path}', null in the payload"
            ));
        }
        // A bare string, so a quoted `"octocat"` never reaches the prompt.
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    Ok(truncate(rendered))
}

/// Clamp one substituted value to [`MAX_SUBSTITUTION`], on a char boundary.
fn truncate(mut value: String) -> String {
    if value.len() <= MAX_SUBSTITUTION {
        return value;
    }
    let mut end = MAX_SUBSTITUTION;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value.push_str(" […truncated]");
    value
}

/// Verify a webhook's `sha256=<hex>` HMAC signature over the raw request body.
///
/// Constant-time comparison: a byte-by-byte early return would leak the
/// expected digest to anyone willing to time their requests. An absent or
/// malformed header is a rejection, so an unsigned request can never be
/// mistaken for a signed one.
pub fn verify_webhook_signature(secret: &str, body: &[u8], header: Option<&str>) -> bool {
    use hmac::Mac as _;

    let Some(header) = header else { return false };
    let Some(hex_digest) = header.trim().strip_prefix("sha256=") else {
        return false;
    };
    let Ok(provided) = hex::decode(hex_digest) else {
        return false;
    };
    let Ok(mut mac) = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    // `verify_slice` compares in constant time and checks the length itself.
    mac.verify_slice(&provided).is_ok()
}

/// The automation runtime: stored triggers, the tick that fires them, and the
/// run history they leave behind (#220).
///
/// Spawns through [`crate::fleet_provider::FleetProvider`] rather than a
/// concrete backend, so the same engine drives caliband over Unix sockets and
/// `CalibanTask` CRs under Kubernetes. It needs a **shared** [`ConfigStore`]:
/// the per-tick claim is what keeps replicas from double-firing, and a config
/// store local to one process could not arbitrate between them.
pub struct AutomationEngine {
    config: std::sync::Arc<dyn crate::config_store::ConfigStore>,
    fleet: std::sync::Arc<dyn crate::fleet_provider::FleetProvider>,
}

impl AutomationEngine {
    /// Build an engine over a shared config store and a fleet backend.
    pub fn new(
        config: std::sync::Arc<dyn crate::config_store::ConfigStore>,
        fleet: std::sync::Arc<dyn crate::fleet_provider::FleetProvider>,
    ) -> Self {
        Self { config, fleet }
    }

    /// Evaluate due automations every `interval` until `shutdown` flips.
    ///
    /// A loop of its own rather than a step inside the fleet poll: how often
    /// schedules are checked and how often agents are reconciled are unrelated
    /// questions, and under k8s there is no fleet poll loop at all.
    pub async fn run(
        self: std::sync::Arc<Self>,
        interval: std::time::Duration,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut shutdown = shutdown;
        if *shutdown.borrow_and_update() {
            return;
        }
        loop {
            self.tick_automations(chrono::Utc::now()).await;
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = shutdown.changed() => break,
            }
        }
        tracing::info!(target: "prospero_automation", "automation loop stopped");
    }

    /// Every configured automation, ordered by id (#220).
    pub async fn list_automations(&self) -> Result<Vec<StoredAutomation>> {
        self.config.list_automations().await
    }

    /// Store a new automation, minting a signing key for a webhook trigger.
    ///
    /// The key is returned once, here — nothing reads it back out afterwards,
    /// so an operator who loses it recreates the automation rather than
    /// recovering the secret.
    pub async fn create_automation(
        &self,
        body: CreateAutomationBody,
    ) -> Result<CreatedAutomationResponse> {
        self.create_automation_at(body, chrono::Utc::now()).await
    }

    /// [`Self::create_automation`] with the creation time supplied.
    ///
    /// `created_at` is the scheduling anchor, so injecting it is what lets a
    /// caller — a test, or a future import/restore path — place an automation
    /// on the timeline instead of always at "now".
    pub async fn create_automation_at(
        &self,
        body: CreateAutomationBody,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<CreatedAutomationResponse> {
        // Reject a bad schedule now, while someone is watching. Accepting it
        // would produce an automation that simply never fires, with nothing
        // anywhere to say why.
        if let Trigger::Cron { schedule } = &body.trigger {
            parse_schedule(schedule).map_err(CoreError::InvalidConfig)?;
        }
        if self
            .config
            .list_automations()
            .await?
            .iter()
            .any(|s| s.automation.id == body.id)
        {
            return Err(CoreError::Conflict(format!(
                "automation '{}' already exists",
                body.id
            )));
        }

        // Only a webhook trigger gets a key, and only at creation — there is no
        // read-back path, so the key cannot later be recovered from the store
        // by anything short of database access.
        let webhook_secret = match body.trigger {
            Trigger::Webhook => Some(mint_webhook_secret()),
            Trigger::Cron { .. } => None,
        };
        let stored = StoredAutomation {
            automation: Automation {
                id: body.id,
                workspace: body.workspace,
                trigger: body.trigger,
                template: body.template,
                enabled: body.enabled,
                created_at: stamp(created_at),
                last_fired_at: None,
            },
            webhook_secret,
        };
        self.config.upsert_automation(&stored).await?;
        Ok(CreatedAutomationResponse {
            automation: stored.automation,
            webhook_secret: stored.webhook_secret,
        })
    }

    /// Look one automation up by id.
    async fn automation(&self, id: &str) -> Result<StoredAutomation> {
        self.config
            .list_automations()
            .await?
            .into_iter()
            .find(|s| s.automation.id == id)
            .ok_or_else(|| CoreError::AutomationNotFound(id.to_string()))
    }

    /// Remove an automation and its run history.
    pub async fn delete_automation(&self, id: &str) -> Result<bool> {
        self.config.delete_automation(id).await
    }

    /// Enable or disable an automation without discarding it.
    pub async fn set_automation_enabled(&self, id: &str, enabled: bool) -> Result<()> {
        let mut stored = self.automation(id).await?;
        stored.automation.enabled = enabled;
        // `upsert_automation` leaves `last_fired_at` alone, so disabling and
        // re-enabling does not replay the ticks that elapsed meanwhile.
        self.config.upsert_automation(&stored).await
    }

    /// An automation's recent runs, newest first.
    pub async fn automation_runs(&self, id: &str, limit: usize) -> Result<Vec<AutomationRun>> {
        self.config.list_runs(id, limit).await
    }

    /// Fire every cron automation whose tick has come due at `now`.
    ///
    /// Safe to run on every replica concurrently: the store's compare-and-set
    /// on the tick decides who acts, so this needs no lifecycle lease. That
    /// also means a scheduled spawn still happens when the lease holder is the
    /// replica that just died.
    pub async fn tick_automations(&self, now: chrono::DateTime<chrono::Utc>) {
        let automations = match self.config.list_automations().await {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(target: "prospero_automation", error = %e, "listing automations failed");
                return;
            }
        };

        for stored in automations {
            let a = &stored.automation;
            if !a.enabled {
                continue;
            }
            let Trigger::Cron { schedule } = &a.trigger else {
                continue;
            };
            // Before the first fire the anchor is creation time, so a new
            // automation waits for its next tick rather than firing at once.
            let anchor_raw = a.last_fired_at.as_deref().unwrap_or(a.created_at.as_str());
            let Ok(anchor) = chrono::DateTime::parse_from_rfc3339(anchor_raw) else {
                tracing::warn!(
                    target: "prospero_automation",
                    automation = %a.id, anchor = %anchor_raw,
                    "unparseable automation timestamp; skipping"
                );
                continue;
            };
            let Some(tick) = due_fire(schedule, anchor.with_timezone(&chrono::Utc), now) else {
                continue;
            };

            // Claim before spawning. Whoever wins owns this tick, and the
            // claim stands even if the spawn then fails — a broken automation
            // records one failed run per tick instead of retrying on every
            // poll forever.
            let fire_at = stamp(tick);
            match self.config.claim_automation_fire(&a.id, &fire_at).await {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    tracing::warn!(
                        target: "prospero_automation",
                        automation = %a.id, error = %e,
                        "claiming an automation tick failed"
                    );
                    continue;
                }
            }

            let run = self
                .run_automation(&stored, fire_at, RunSource::Schedule, None)
                .await;
            self.save_run(&run).await;
        }
    }

    /// Perform an automation's spawn and describe what happened.
    ///
    /// Never returns an error: a failure *is* the outcome being recorded.
    async fn run_automation(
        &self,
        stored: &StoredAutomation,
        fired_at: String,
        source: RunSource,
        payload: Option<&serde_json::Value>,
    ) -> AutomationRun {
        let a = &stored.automation;
        let mut run = AutomationRun {
            automation_id: a.id.clone(),
            fired_at,
            source,
            agent_id: None,
            error: None,
        };

        let task = match payload {
            Some(payload) => match interpolate(&a.template.task, payload) {
                Ok(task) => task,
                Err(e) => {
                    run.error = Some(e);
                    return run;
                }
            },
            None => a.template.task.clone(),
        };

        let spec = crate::model::TaskSpec {
            workspace: a.workspace.clone(),
            request: spawn_request_from(&a.template, task),
        };
        match self.fleet.ensure_agent(spec).await {
            Ok(handle) => run.agent_id = Some(handle.id.to_string()),
            Err(e) => run.error = Some(e.to_string()),
        }
        run
    }

    /// Persist a run, logging rather than propagating a store failure — the
    /// agent has already been spawned by this point, so failing the caller
    /// would misreport what happened.
    async fn save_run(&self, run: &AutomationRun) {
        if let Err(e) = self.config.record_run(run).await {
            tracing::warn!(
                target: "prospero_automation",
                automation = %run.automation_id, error = %e,
                "recording an automation run failed"
            );
        }
    }

    /// Fire one automation immediately, whatever its trigger.
    ///
    /// Backs both "run now" and an accepted webhook. `payload` fills the task
    /// template's placeholders; `None` means the template must have none.
    /// Records a run either way — including when the spawn fails, because an
    /// automation that quietly stops working is the failure this history is
    /// for.
    pub async fn fire_automation(
        &self,
        id: &str,
        source: RunSource,
        payload: Option<&serde_json::Value>,
    ) -> Result<AutomationRun> {
        let stored = self.automation(id).await?;
        let fired_at = stamp(chrono::Utc::now());
        let run = self
            .run_automation(&stored, fired_at, source, payload)
            .await;
        self.save_run(&run).await;
        Ok(run)
    }

    /// Verify a webhook's signature and, if it holds, fire the automation.
    ///
    /// `body` is the **raw** request bytes: the signature covers exactly what
    /// the sender transmitted, so re-serializing parsed JSON first would
    /// compare a different byte string and reject every honest caller.
    pub async fn trigger_webhook(
        &self,
        id: &str,
        body: &[u8],
        signature: Option<&str>,
    ) -> Result<AutomationRun> {
        let stored = self.automation(id).await?;

        // A cron automation has no key, so there is nothing a caller could
        // sign with — reject rather than treating "no secret" as "no check".
        let Some(secret) = stored.webhook_secret.as_deref() else {
            return Err(CoreError::Unauthorized(format!(
                "automation '{id}' has no webhook trigger"
            )));
        };
        if !verify_webhook_signature(secret, body, signature) {
            return Err(CoreError::Unauthorized(format!(
                "bad or missing signature for automation '{id}'"
            )));
        }
        if !stored.automation.enabled {
            return Err(CoreError::Conflict(format!(
                "automation '{id}' is disabled"
            )));
        }

        let payload: serde_json::Value = serde_json::from_slice(body)?;
        let fired_at = stamp(chrono::Utc::now());
        let run = self
            .run_automation(&stored, fired_at, RunSource::Webhook, Some(&payload))
            .await;
        self.save_run(&run).await;
        Ok(run)
    }
}

/// Build the spawn an automation's template describes.
///
/// Everything an automation can ask for is a field a person could set on a
/// manual spawn, so an automation is never a way to reach a capability the
/// HTTP API does not already offer.
fn spawn_request_from(template: &SpawnTemplate, task: String) -> crate::fleet::SpawnRequest {
    crate::fleet::SpawnRequest {
        prompt: task,
        label: template.label.clone(),
        model: template.model.clone(),
        isolation_worktree: template.isolation_worktree,
        tool_allowlist: template.tool_allowlist.clone(),
        // An automation runs with nobody at the keyboard, so an interactive
        // agent would simply sit waiting for input that never comes.
        interactive: false,
        frontmatter_path: template
            .frontmatter_path
            .as_ref()
            .map(std::path::PathBuf::from),
        provider_ref: template.provider_ref.clone(),
        permission_posture: template.permission_posture,
        timeout_secs: template.timeout_secs,
    }
}

/// Mint a webhook signing key: 32 bytes of CSPRNG output, hex-encoded.
fn mint_webhook_secret() -> String {
    use rand::RngCore as _;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn fires_the_first_tick_after_the_anchor() {
        let fire = due_fire(
            "*/5 * * * *",
            t("2026-09-20T10:00:00Z"),
            t("2026-09-20T10:05:30Z"),
        );
        assert_eq!(fire, Some(t("2026-09-20T10:05:00Z")));
    }

    #[test]
    fn does_not_fire_between_ticks() {
        assert_eq!(
            due_fire(
                "*/5 * * * *",
                t("2026-09-20T10:00:00Z"),
                t("2026-09-20T10:03:00Z")
            ),
            None
        );
    }

    #[test]
    fn missed_ticks_coalesce_into_a_single_catch_up_fire() {
        // Daemon down 10:00 -> 11:02 on a 5-minute schedule. Firing every
        // missed tick would launch 12 agents at once; we owe exactly one.
        let fire = due_fire(
            "*/5 * * * *",
            t("2026-09-20T10:00:00Z"),
            t("2026-09-20T11:02:00Z"),
        );
        assert_eq!(fire, Some(t("2026-09-20T11:00:00Z")));
    }

    #[test]
    fn a_rejected_schedule_is_reported_not_silently_never_fired() {
        assert!(parse_schedule("not a cron expression").is_err());
        assert!(parse_schedule("*/5 * * * *").is_ok());
    }

    #[test]
    fn an_invalid_schedule_never_claims_to_be_due() {
        assert_eq!(
            due_fire(
                "nonsense",
                t("2026-09-20T10:00:00Z"),
                t("2026-09-20T11:00:00Z")
            ),
            None
        );
    }

    #[test]
    fn placeholders_take_their_values_from_the_payload() {
        let payload = serde_json::json!({
            "issue": { "number": 42, "title": "the bug" },
            "actor": "octocat",
        });
        let out = interpolate(
            "Fix {{ issue.title }} (#{{issue.number}}) reported by {{ actor }}",
            &payload,
        )
        .unwrap();
        assert_eq!(out, "Fix the bug (#42) reported by octocat");
    }

    #[test]
    fn a_missing_field_fails_the_run_instead_of_spawning_a_broken_prompt() {
        let payload = serde_json::json!({ "actor": "octocat" });
        let err = interpolate("Fix {{ issue.title }}", &payload).unwrap_err();
        assert!(
            err.contains("issue.title"),
            "error should name the path: {err}"
        );

        let null = serde_json::json!({ "issue": { "title": null } });
        assert!(interpolate("Fix {{ issue.title }}", &null).is_err());
    }

    #[test]
    fn an_oversized_payload_field_is_truncated_not_passed_through() {
        let payload = serde_json::json!({ "body": "x".repeat(MAX_SUBSTITUTION * 2) });
        let out = interpolate("{{ body }}", &payload).unwrap();
        assert!(
            out.len() < MAX_SUBSTITUTION * 2,
            "a caller-controlled field must not set the prompt size"
        );
        assert!(
            out.contains("truncated"),
            "truncation must be visible: {out}"
        );
    }

    /// The signature the sender computes, for tests that need a valid one.
    fn sign(secret: &str, body: &[u8]) -> String {
        use hmac::Mac as _;
        let mut mac =
            hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).expect("any key length");
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn a_correctly_signed_body_is_accepted() {
        let body = br#"{"hello":"world"}"#;
        let sig = sign("s3cret", body);
        assert!(verify_webhook_signature("s3cret", body, Some(&sig)));
    }

    #[test]
    fn an_unsigned_or_wrongly_signed_request_is_rejected() {
        let body = br#"{"hello":"world"}"#;
        let good = sign("s3cret", body);
        // No header at all — the acceptance criterion for an unsigned request.
        assert!(!verify_webhook_signature("s3cret", body, None));
        // Right shape, wrong key.
        assert!(!verify_webhook_signature("other", body, Some(&good)));
        // Right key, body tampered with after signing.
        assert!(!verify_webhook_signature(
            "s3cret",
            br#"{"hello":"evil"}"#,
            Some(&good)
        ));
        // Malformed headers must not be mistaken for a signature.
        assert!(!verify_webhook_signature("s3cret", body, Some("")));
        assert!(!verify_webhook_signature("s3cret", body, Some("garbage")));
        assert!(!verify_webhook_signature(
            "s3cret",
            body,
            Some(&good.replace("sha256=", ""))
        ));
    }

    #[test]
    fn five_field_cron_gains_a_zero_seconds_field() {
        // Without this, the `cron` crate reads "*/5 * * * *" as every 5
        // *seconds* — a 12x over-fire that no operator asked for.
        assert_eq!(normalize_schedule("*/5 * * * *"), "0 */5 * * * *");
        assert_eq!(normalize_schedule("30 2 * * 1"), "0 30 2 * * 1");
    }

    #[test]
    fn six_and_seven_field_cron_pass_through() {
        assert_eq!(normalize_schedule("15 */5 * * * *"), "15 */5 * * * *");
        assert_eq!(
            normalize_schedule("0 0 12 * * Mon 2030"),
            "0 0 12 * * Mon 2030"
        );
    }
    /// #220 acceptance criterion 1: a cron automation spawns on schedule and
    /// records the run.
    #[tokio::test]
    async fn a_due_cron_automation_spawns_and_records_the_run() {
        let (engine, fake, _dir) = automation_fixture().await;
        create(&engine, cron_body("nightly", "*/5 * * * *"))
            .await
            .unwrap();

        engine.tick_automations(at("2026-09-20T10:05:30Z")).await;

        let specs = fake.received_specs();
        assert_eq!(specs.len(), 1, "exactly one agent spawned");
        assert_eq!(specs[0].initial_prompt, "sweep the logs");

        let runs = engine.automation_runs("nightly", 10).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].source, RunSource::Schedule);
        assert_eq!(
            runs[0].fired_at, "2026-09-20T10:05:00Z",
            "the tick, not now"
        );
        assert!(runs[0].agent_id.is_some(), "the run names the agent");
        assert!(runs[0].error.is_none());
    }

    /// The tick is claimed, so re-running the same moment — a second replica,
    /// or the next poll before the next tick — must not spawn again.
    #[tokio::test]
    async fn the_same_tick_never_fires_twice() {
        let (engine, fake, _dir) = automation_fixture().await;
        create(&engine, cron_body("nightly", "*/5 * * * *"))
            .await
            .unwrap();

        for _ in 0..3 {
            engine.tick_automations(at("2026-09-20T10:05:30Z")).await;
        }

        assert_eq!(fake.received_specs().len(), 1, "one tick, one agent");
        assert_eq!(
            engine.automation_runs("nightly", 10).await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn an_automation_does_not_fire_before_its_tick_or_while_disabled() {
        let (engine, fake, _dir) = automation_fixture().await;
        create(&engine, cron_body("nightly", "*/5 * * * *"))
            .await
            .unwrap();

        // Anchored at 10:00; 10:03 is not yet a tick.
        engine.tick_automations(at("2026-09-20T10:03:00Z")).await;
        assert!(fake.received_specs().is_empty(), "fired between ticks");

        engine
            .set_automation_enabled("nightly", false)
            .await
            .unwrap();
        engine.tick_automations(at("2026-09-20T10:05:30Z")).await;
        assert!(
            fake.received_specs().is_empty(),
            "a disabled automation fired"
        );

        // Re-enabling lets the next due tick through.
        engine
            .set_automation_enabled("nightly", true)
            .await
            .unwrap();
        engine.tick_automations(at("2026-09-20T10:10:30Z")).await;
        assert_eq!(fake.received_specs().len(), 1);
    }

    /// #220 acceptance criterion 2: a signed webhook spawns with payload fields
    /// in the task; an unsigned one is rejected.
    #[tokio::test]
    async fn a_signed_webhook_spawns_with_payload_fields_in_the_task() {
        let (engine, fake, _dir) = automation_fixture().await;
        let created = create(
            &engine,
            CreateAutomationBody {
                id: "on-issue".into(),
                workspace: "p".into(),
                trigger: Trigger::Webhook,
                template: SpawnTemplate {
                    task: "Fix {{ issue.title }} for {{ actor }}".into(),
                    ..Default::default()
                },
                enabled: true,
            },
        )
        .await
        .unwrap();
        let secret = created
            .webhook_secret
            .expect("a webhook automation is issued a signing key");

        let body = br#"{"issue":{"title":"the bug"},"actor":"octocat"}"#;
        let signature = {
            use hmac::Mac as _;
            let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
            mac.update(body);
            format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
        };

        let run = engine
            .trigger_webhook("on-issue", body, Some(&signature))
            .await
            .unwrap();
        assert_eq!(run.source, RunSource::Webhook);
        assert!(run.error.is_none());

        let specs = fake.received_specs();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].initial_prompt, "Fix the bug for octocat");
    }

    #[tokio::test]
    async fn an_unsigned_or_mis_signed_webhook_spawns_nothing() {
        let (engine, fake, _dir) = automation_fixture().await;
        create(
            &engine,
            CreateAutomationBody {
                id: "on-issue".into(),
                workspace: "p".into(),
                trigger: Trigger::Webhook,
                template: SpawnTemplate {
                    task: "Fix {{ issue.title }}".into(),
                    ..Default::default()
                },
                enabled: true,
            },
        )
        .await
        .unwrap();

        let body = br#"{"issue":{"title":"the bug"}}"#;
        assert!(
            engine
                .trigger_webhook("on-issue", body, None)
                .await
                .is_err(),
            "an unsigned request must be rejected"
        );
        assert!(
            engine
                .trigger_webhook("on-issue", body, Some("sha256=deadbeef"))
                .await
                .is_err(),
            "a bad signature must be rejected"
        );
        assert!(
            fake.received_specs().is_empty(),
            "a rejected webhook spawned an agent"
        );
        assert!(
            engine
                .automation_runs("on-issue", 10)
                .await
                .unwrap()
                .is_empty(),
            "a rejected webhook must not even be recorded as a run"
        );
    }

    /// A cron automation cannot be fired by webhook, however well signed: its
    /// trigger is the schedule, and it was never issued a key.
    #[tokio::test]
    async fn a_cron_automation_has_no_webhook_surface() {
        let (engine, _fake, _dir) = automation_fixture().await;
        let created = create(&engine, cron_body("nightly", "*/5 * * * *"))
            .await
            .unwrap();
        assert!(
            created.webhook_secret.is_none(),
            "a cron automation must not be issued a signing key"
        );
        assert!(
            engine
                .trigger_webhook("nightly", b"{}", Some("sha256=00"))
                .await
                .is_err()
        );
    }

    /// A failing spawn is still recorded, so an automation that has quietly
    /// stopped working is visible in its history rather than silent.
    #[tokio::test]
    async fn a_failed_spawn_is_recorded_as_a_failed_run() {
        let (engine, _fake, _dir) = automation_fixture().await;
        create(
            &engine,
            CreateAutomationBody {
                id: "ghost".into(),
                workspace: "not-registered".into(),
                trigger: Trigger::Cron {
                    schedule: "*/5 * * * *".into(),
                },
                template: SpawnTemplate {
                    task: "sweep the logs".into(),
                    ..Default::default()
                },
                enabled: true,
            },
        )
        .await
        .unwrap();

        engine.tick_automations(at("2026-09-20T10:05:30Z")).await;

        let runs = engine.automation_runs("ghost", 10).await.unwrap();
        assert_eq!(runs.len(), 1, "a failed fire is still a run");
        assert!(runs[0].agent_id.is_none());
        assert!(runs[0].error.is_some(), "the run says why it failed");

        // And the tick stays claimed, so a broken automation does not retry in
        // a tight loop on every poll.
        engine.tick_automations(at("2026-09-20T10:05:40Z")).await;
        assert_eq!(engine.automation_runs("ghost", 10).await.unwrap().len(), 1);
    }

    /// Every automation in these tests is anchored here, so a fixed `now`
    /// passed to `tick_automations` is a real elapsed interval rather than a
    /// moment relative to the wall clock.
    const ANCHOR: &str = "2026-09-20T10:00:00Z";

    async fn create(
        engine: &AutomationEngine,
        body: CreateAutomationBody,
    ) -> Result<CreatedAutomationResponse> {
        engine.create_automation_at(body, at(ANCHOR)).await
    }

    fn at(s: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn cron_body(id: &str, schedule: &str) -> CreateAutomationBody {
        CreateAutomationBody {
            id: id.into(),
            workspace: "p".into(),
            trigger: Trigger::Cron {
                schedule: schedule.into(),
            },
            template: SpawnTemplate {
                task: "sweep the logs".into(),
                ..Default::default()
            },
            enabled: true,
        }
    }

    /// An engine over a real `LocalFleet` backed by `FakeCaliband`, with
    /// workspace "p" registered. Driving the actual `FleetProvider` seam means
    /// a passing test here is a spawn that would really reach a backend.
    async fn automation_fixture() -> (
        AutomationEngine,
        crate::testkit::FakeCaliband,
        tempfile::TempDir,
    ) {
        use crate::testkit::FakeCaliband;

        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::fleet::FleetConfig::new("local", dir.path());
        config.discovery_env.caliban_daemon_runtime_dir = Some(dir.path().to_path_buf());
        config.ensure.autostart = false;
        let root = dir.path().join("repo");
        std::fs::create_dir_all(&root).unwrap();
        let socket = crate::discovery::resolve_socket(&root, &config.discovery_env).unwrap();
        let fake = FakeCaliband::start_at(&socket).await.unwrap();
        let store = std::sync::Arc::new(crate::store::JsonlStore::open(dir.path()).unwrap());
        let config_store: std::sync::Arc<dyn crate::config_store::ConfigStore> =
            std::sync::Arc::new(
                crate::config_store::SqliteConfigStore::open(dir.path())
                    .await
                    .unwrap(),
            );
        let mgr =
            crate::fleet::FleetManager::with_config_store(config, store, config_store.clone())
                .await
                .unwrap();
        mgr.add_repo("p", &root).await.unwrap();
        let fleet: std::sync::Arc<dyn crate::fleet_provider::FleetProvider> =
            std::sync::Arc::new(crate::fleet_provider::LocalFleet::new(mgr));
        (AutomationEngine::new(config_store, fleet), fake, dir)
    }
}
