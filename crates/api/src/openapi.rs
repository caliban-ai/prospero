//! The REST surface as an OpenAPI 3.1 document (#222).
//!
//! Clients — [ariel](https://github.com/caliban-ai/ariel) first — otherwise have
//! to hand-write every request and response type by reading the guide. This
//! module publishes the same shapes as a machine-readable contract at
//! `GET /api/openapi.json`.
//!
//! # Why 3.1, and why `schemars` rather than a spec framework
//!
//! OpenAPI 3.1 dropped its bespoke schema dialect and adopted JSON Schema
//! 2020-12 wholesale — which is exactly what `schemars` emits. So the derives on
//! `prospero-types` drop into `components/schemas` untouched. Targeting 3.0
//! instead would mean running schemars' downgrade transforms (`AddNullable`,
//! `RemoveRefSiblings`, …), each of which loses something the real type says.
//! Publishing 3.1 is therefore not a version preference; it is what lets the
//! document describe the types as they actually are.
//!
//! The schemas come from the same structs the handlers serialize, so they cannot
//! drift from the wire. The *routes* are a hand-written table below, because
//! axum's `Router` cannot be enumerated at runtime — but `tests/openapi.rs`
//! scrapes `lib.rs` for both the paths it registers and the verbs it answers on
//! each, so a route or method missing from this table fails the build rather
//! than shipping a spec that quietly lies.

use schemars::SchemaGenerator;
use schemars::generate::{Contract, SchemaSettings};
use serde_json::{Map, Value, json};

/// What a request or response carries.
enum Body {
    /// No body at all (`204`, `202`).
    Empty,
    /// A single instance of a named component schema.
    Json(&'static str),
    /// An array of a named component schema.
    JsonArray(&'static str),
    /// `text/plain`.
    Text,
    /// A Server-Sent Events stream.
    Sse,
}

/// One documented response.
struct Res {
    status: &'static str,
    description: &'static str,
    body: Body,
}

/// A query-string parameter. Path parameters are derived from the path
/// template instead, so they cannot be forgotten.
struct Query {
    name: &'static str,
    description: &'static str,
    /// JSON Schema type for the raw query value.
    ty: &'static str,
}

/// One operation on a path.
struct Op {
    method: &'static str,
    id: &'static str,
    summary: &'static str,
    /// The minimum scope `auth::required_access` enforces, surfaced so a client
    /// can tell which token it needs without reading the server source.
    scope: &'static str,
    query: &'static [Query],
    request: Option<&'static str>,
    responses: &'static [Res],
}

/// Standard error responses every fallible route shares.
const ERRORS: &[Res] = &[
    Res {
        status: "4XX",
        description: "Request rejected: see the `kind` field for which.",
        body: Body::Json("ApiErrorBody"),
    },
    Res {
        status: "5XX",
        description: "prosperod or a backend it depends on failed.",
        body: Body::Json("ApiErrorBody"),
    },
];

const OK_EMPTY: &str = "Applied.";

/// Every documented path, in the order the router declares them.
const ROUTES: &[(&str, &[Op])] = &[
    (
        "/healthz",
        &[Op {
            method: "get",
            id: "healthz",
            summary: "Liveness — always 200 while the process is up.",
            scope: "open",
            query: &[],
            request: None,
            responses: &[Res {
                status: "200",
                description: "The literal body `ok`.",
                body: Body::Text,
            }],
        }],
    ),
    (
        "/readyz",
        &[Op {
            method: "get",
            id: "readyz",
            summary: "Readiness — 200 when the event store is writable.",
            scope: "open",
            query: &[],
            request: None,
            responses: &[
                Res {
                    status: "200",
                    description: "Ready to serve.",
                    body: Body::Json("Readiness"),
                },
                Res {
                    status: "503",
                    description: "Not ready; the same body explains why.",
                    body: Body::Json("Readiness"),
                },
            ],
        }],
    ),
    (
        "/api/session",
        &[
            Op {
                method: "get",
                id: "getSession",
                summary: "Who the current credential belongs to.",
                scope: "open",
                query: &[],
                request: None,
                responses: &[Res {
                    status: "200",
                    description: "The signed-in principal, or anonymous.",
                    body: Body::Json("SessionInfo"),
                }],
            },
            Op {
                method: "post",
                id: "signIn",
                summary: "Exchange a token for the dashboard session cookie.",
                scope: "open",
                query: &[],
                request: Some("SignInBody"),
                responses: &[Res {
                    status: "200",
                    description: "Signed in; the session cookie is set.",
                    body: Body::Json("SessionInfo"),
                }],
            },
            Op {
                method: "delete",
                id: "signOut",
                summary: "Clear the session cookie.",
                scope: "open",
                query: &[],
                request: None,
                responses: &[Res {
                    status: "204",
                    description: "Signed out.",
                    body: Body::Empty,
                }],
            },
        ],
    ),
    (
        "/api/openapi.json",
        &[Op {
            method: "get",
            id: "getOpenapi",
            summary: "This document.",
            scope: "open",
            query: &[],
            request: None,
            responses: &[Res {
                status: "200",
                description: "The OpenAPI 3.1 description of this server.",
                body: Body::Json("OpenApiDocument"),
            }],
        }],
    ),
    (
        "/api/metrics",
        &[Op {
            method: "get",
            id: "getMetrics",
            summary: "Point-in-time operational counters.",
            scope: "read",
            query: &[],
            request: None,
            responses: &[Res {
                status: "200",
                description: "Counter snapshot.",
                body: Body::Json("MetricsSnapshot"),
            }],
        }],
    ),
    (
        "/api/capabilities",
        &[Op {
            method: "get",
            id: "getCapabilities",
            summary: "What the active fleet backend supports.",
            scope: "read",
            query: &[],
            request: None,
            responses: &[Res {
                status: "200",
                description: "Backend capabilities.",
                body: Body::Json("Capabilities"),
            }],
        }],
    ),
    (
        "/api/fleet",
        &[Op {
            method: "get",
            id: "getFleet",
            summary: "Every workspace and agent in the fleet.",
            scope: "read",
            query: &[],
            request: None,
            responses: &[Res {
                status: "200",
                description: "Fleet snapshot.",
                body: Body::Json("FleetSnapshot"),
            }],
        }],
    ),
    (
        "/api/fleet/stream",
        &[Op {
            method: "get",
            id: "streamFleet",
            summary: "Fleet-wide event stream (SSE).",
            scope: "read",
            query: &[Query {
                name: "from",
                description: "A fleet cursor to resume after, or `now` to tail \
                              only what happens next. Absent replays from the start.",
                ty: "string",
            }],
            request: None,
            responses: &[Res {
                status: "200",
                description: "An endless `text/event-stream` of `FleetEvent` frames.",
                body: Body::Sse,
            }],
        }],
    ),
    (
        "/api/usage",
        &[Op {
            method: "get",
            id: "getUsage",
            summary: "Token and cost usage, bucketed by workspace and day.",
            scope: "read",
            query: &[
                Query {
                    name: "since",
                    description: "Inclusive window start (RFC-3339, any offset). \
                                  Echoed back normalized to UTC.",
                    ty: "string",
                },
                Query {
                    name: "until",
                    description: "Exclusive window end (RFC-3339, any offset). \
                                  Echoed back normalized to UTC.",
                    ty: "string",
                },
                Query {
                    name: "days",
                    description: "How many days back to look, resolved against the \
                                  server's clock. Ignored when `since` is given. \
                                  Bounded: a window past any real history is a 400.",
                    ty: "integer",
                },
            ],
            request: None,
            responses: &[
                Res {
                    status: "200",
                    description: "Usage report, echoing the window actually used.",
                    body: Body::Json("UsageReport"),
                },
                Res {
                    status: "400",
                    description: "`since` or `until` is not an RFC-3339 timestamp, or \
                                  `days` is past the server's limit (`kind`: \
                                  `bad_request`).",
                    body: Body::Json("ApiErrorBody"),
                },
            ],
        }],
    ),
    (
        "/api/workspaces",
        &[
            Op {
                method: "get",
                id: "listWorkspaces",
                summary: "Registered workspaces.",
                scope: "read",
                query: &[],
                request: None,
                responses: &[Res {
                    status: "200",
                    description: "One summary per workspace.",
                    body: Body::JsonArray("WorkspaceSummary"),
                }],
            },
            Op {
                method: "post",
                id: "addWorkspace",
                summary: "Register a workspace.",
                scope: "admin",
                query: &[],
                request: Some("AddWorkspaceBody"),
                responses: &[
                    Res {
                        status: "201",
                        description: "Created — a synchronous backend applied it already.",
                        body: Body::Empty,
                    },
                    Res {
                        status: "202",
                        description: "Accepted — an async backend (k8s) enqueued a reconcile.",
                        body: Body::Empty,
                    },
                ],
            },
        ],
    ),
    (
        "/api/workspaces/{name}",
        &[Op {
            method: "delete",
            id: "removeWorkspace",
            summary: "Unregister a workspace.",
            scope: "admin",
            query: &[],
            request: None,
            responses: &[Res {
                status: "204",
                description: OK_EMPTY,
                body: Body::Empty,
            }],
        }],
    ),
    (
        "/api/workspaces/{name}/config",
        &[Op {
            method: "put",
            id: "setWorkspaceConfig",
            summary: "Replace a workspace's provider configuration.",
            scope: "admin",
            query: &[],
            request: Some("SetConfigBody"),
            responses: &[
                Res {
                    status: "204",
                    description: "Applied synchronously.",
                    body: Body::Empty,
                },
                Res {
                    status: "202",
                    description: "Accepted — an async backend will re-reconcile.",
                    body: Body::Empty,
                },
            ],
        }],
    ),
    (
        "/api/workspaces/{workspace}/agents",
        &[
            Op {
                method: "get",
                id: "listWorkspaceAgents",
                summary: "Agents under one workspace.",
                scope: "read",
                query: &[],
                request: None,
                responses: &[Res {
                    status: "200",
                    description: "The workspace's agents.",
                    body: Body::JsonArray("Agent"),
                }],
            },
            Op {
                method: "post",
                id: "spawnAgent",
                summary: "Spawn an agent in this workspace.",
                scope: "operate",
                query: &[],
                request: Some("SpawnBody"),
                responses: &[Res {
                    status: "201",
                    description: "Spawned.",
                    body: Body::Json("SpawnedResponse"),
                }],
            },
        ],
    ),
    (
        "/api/agents/{id}",
        &[
            Op {
                method: "get",
                id: "getAgent",
                summary: "One agent.",
                scope: "read",
                query: &[],
                request: None,
                responses: &[Res {
                    status: "200",
                    description: "The agent.",
                    body: Body::Json("Agent"),
                }],
            },
            Op {
                method: "delete",
                id: "removeAgent",
                summary: "Forget a finished agent.",
                scope: "operate",
                query: &[],
                request: None,
                responses: &[Res {
                    status: "204",
                    description: OK_EMPTY,
                    body: Body::Empty,
                }],
            },
        ],
    ),
    (
        "/api/agents/{id}/events",
        &[Op {
            method: "get",
            id: "getAgentEvents",
            summary: "Durable event history for one agent.",
            scope: "read",
            query: &[Query {
                name: "from",
                description: "Return events with `seq >= from` (default 0).",
                ty: "integer",
            }],
            request: None,
            responses: &[Res {
                status: "200",
                description: "Events in sequence order.",
                body: Body::JsonArray("FleetEvent"),
            }],
        }],
    ),
    (
        "/api/agents/{id}/stream",
        &[Op {
            method: "get",
            id: "streamAgent",
            summary: "One agent's live event stream (SSE).",
            scope: "read",
            query: &[Query {
                name: "from",
                description: "Replay from this `seq` before tailing (default 0).",
                ty: "integer",
            }],
            request: None,
            responses: &[Res {
                status: "200",
                description: "A `text/event-stream` of `FleetEvent` frames.",
                body: Body::Sse,
            }],
        }],
    ),
    (
        "/api/agents/{id}/kill",
        &[Op {
            method: "post",
            id: "killAgent",
            summary: "Stop a running agent.",
            scope: "operate",
            query: &[],
            request: None,
            responses: &[Res {
                status: "202",
                description: "Stop requested.",
                body: Body::Empty,
            }],
        }],
    ),
    (
        "/api/agents/{id}/respawn",
        &[Op {
            method: "post",
            id: "respawnAgent",
            summary: "Start a fresh agent from a finished one's request.",
            scope: "operate",
            query: &[],
            request: None,
            responses: &[Res {
                status: "200",
                description: "The new agent's id.",
                body: Body::Json("RespawnedResponse"),
            }],
        }],
    ),
    (
        "/api/agents/{id}/input",
        &[Op {
            method: "post",
            id: "sendAgentInput",
            summary: "Send a line of input to an interactive agent.",
            scope: "operate",
            query: &[],
            request: Some("AgentInputBody"),
            responses: &[Res {
                status: "202",
                description: "Input forwarded.",
                body: Body::Empty,
            }],
        }],
    ),
    (
        "/api/agents/{id}/end-input",
        &[Op {
            method: "post",
            id: "endAgentInput",
            summary: "Close an interactive agent's input stream.",
            scope: "operate",
            query: &[],
            request: None,
            responses: &[Res {
                status: "202",
                description: "Input closed.",
                body: Body::Empty,
            }],
        }],
    ),
    (
        "/api/automations",
        &[
            Op {
                method: "get",
                id: "listAutomations",
                summary: "Every automation. Never includes webhook signing keys.",
                scope: "read",
                query: &[],
                request: None,
                responses: &[Res {
                    status: "200",
                    description: "The automations.",
                    body: Body::JsonArray("Automation"),
                }],
            },
            Op {
                method: "post",
                id: "createAutomation",
                summary: "Create an automation.",
                scope: "admin",
                query: &[],
                request: Some("CreateAutomationBody"),
                responses: &[Res {
                    status: "201",
                    description: "Created. For a webhook trigger this response is \
                                  the only time the signing key is ever returned.",
                    body: Body::Json("CreatedAutomationResponse"),
                }],
            },
        ],
    ),
    (
        "/api/automations/{id}",
        &[Op {
            method: "delete",
            id: "deleteAutomation",
            summary: "Delete an automation and its run history.",
            scope: "admin",
            query: &[],
            request: None,
            responses: &[Res {
                status: "204",
                description: OK_EMPTY,
                body: Body::Empty,
            }],
        }],
    ),
    (
        "/api/automations/{id}/enabled",
        &[Op {
            method: "put",
            id: "setAutomationEnabled",
            summary: "Enable or disable an automation without deleting it.",
            scope: "admin",
            query: &[],
            request: Some("SetEnabledBody"),
            responses: &[Res {
                status: "204",
                description: OK_EMPTY,
                body: Body::Empty,
            }],
        }],
    ),
    (
        "/api/automations/{id}/runs",
        &[Op {
            method: "get",
            id: "listAutomationRuns",
            summary: "Recent firings of one automation, newest first.",
            scope: "read",
            query: &[Query {
                name: "limit",
                description: "How many runs to return.",
                ty: "integer",
            }],
            request: None,
            responses: &[Res {
                status: "200",
                description: "The run history.",
                body: Body::JsonArray("AutomationRun"),
            }],
        }],
    ),
    (
        "/api/automations/{id}/run",
        &[Op {
            method: "post",
            id: "runAutomation",
            summary: "Fire an automation by hand, ignoring its schedule.",
            scope: "operate",
            query: &[],
            request: None,
            responses: &[Res {
                status: "200",
                description: "The resulting run.",
                body: Body::Json("FiredResponse"),
            }],
        }],
    ),
    (
        "/api/automations/{id}/trigger",
        &[Op {
            method: "post",
            id: "triggerAutomation",
            summary: "Fire a webhook automation.",
            scope: "open (HMAC signature)",
            query: &[],
            request: None,
            responses: &[Res {
                status: "200",
                description: "The resulting run.",
                body: Body::Json("FiredResponse"),
            }],
        }],
    ),
];

/// `GET /api/openapi.json` — serve the document.
///
/// Built per request rather than cached: it is a handful of milliseconds of
/// `serde_json` on a route nothing calls in a hot loop, and a `OnceLock` here
/// would keep a stale document alive across a hot-reload in tests.
pub async fn get_openapi() -> axum::Json<Value> {
    axum::Json(document())
}

/// Suffix distinguishing a type's request shape from its response shape.
const REQUEST_SUFFIX: &str = "Request";

/// A fresh generator emitting OpenAPI-3.1-compatible refs under `contract`.
fn generator_for(contract: Contract) -> SchemaGenerator {
    SchemaGenerator::new(SchemaSettings::draft2020_12().with(|s| {
        s.definitions_path = "/components/schemas".into();
        s.contract = contract;
    }))
}

/// Schemas for everything the server *writes*, under the serialize contract.
///
/// Registering a type also registers everything it references, so these roots
/// pull in the whole reachable graph.
fn response_schemas() -> Map<String, Value> {
    let mut generator = generator_for(Contract::Serialize);
    macro_rules! register {
        ($($ty:ty),* $(,)?) => { $( let _ = generator.subschema_for::<$ty>(); )* };
    }
    register!(
        prospero_core::MetricsSnapshot,
        prospero_core::Readiness,
        prospero_types::Agent,
        prospero_types::Automation,
        prospero_types::AutomationRun,
        prospero_types::Capabilities,
        prospero_types::CreatedAutomationResponse,
        prospero_types::FiredResponse,
        prospero_types::FleetEvent,
        prospero_types::FleetSnapshot,
        prospero_types::RespawnedResponse,
        prospero_types::SessionInfo,
        prospero_types::SpawnedResponse,
        prospero_types::UsageReport,
        prospero_types::WorkspaceSummary,
    );
    generator.take_definitions(true)
}

/// Schemas for everything the server *reads*, under the deserialize contract.
fn request_schemas() -> Map<String, Value> {
    let mut generator = generator_for(Contract::Deserialize);
    macro_rules! register {
        ($($ty:ty),* $(,)?) => { $( let _ = generator.subschema_for::<$ty>(); )* };
    }
    register!(
        prospero_types::AddWorkspaceBody,
        prospero_types::AgentInputBody,
        prospero_types::CreateAutomationBody,
        prospero_types::SetConfigBody,
        prospero_types::SetEnabledBody,
        prospero_types::SignInBody,
        prospero_types::SpawnBody,
    );
    generator.take_definitions(true)
}

/// Merge the two directions into one `components/schemas`.
///
/// `schemars` describes a type differently depending on whether it is being
/// read or written: `#[serde(default)]` makes a field optional to a *reader*
/// but says nothing to a writer, and `skip_serializing_if` does the reverse.
/// Publishing one schema per type would therefore misdescribe one direction —
/// a generated client would either demand a field the server omits or omit one
/// the server demands.
///
/// So each direction is generated under its own contract, and a type is only
/// renamed when the two shapes actually differ. Most types read and write
/// identically and keep their plain name; the handful that do not (a
/// `SpawnTemplate` inside a request body versus inside a stored automation)
/// gain a `Request` twin. The returned map is the merged set; the second value
/// maps a request type's plain name to the name it was published under.
fn merged_schemas() -> (Map<String, Value>, Map<String, Value>) {
    let responses = response_schemas();
    let requests = request_schemas();

    // Decide each request type's published name before rewriting any refs, so
    // a rename is applied consistently wherever it is referenced.
    let mut renames = Map::new();
    for (name, schema) in &requests {
        let collides = responses.get(name).is_some_and(|r| r != schema);
        if collides {
            renames.insert(
                name.clone(),
                Value::String(format!("{name}{REQUEST_SUFFIX}")),
            );
        }
    }

    let mut merged = responses;
    for (name, schema) in requests {
        let published = renames
            .get(&name)
            .and_then(Value::as_str)
            .unwrap_or(&name)
            .to_string();
        merged.insert(published, rename_refs(schema, &renames));
    }
    (merged, renames)
}

/// Rewrite every `$ref` in `schema` that points at a renamed type.
fn rename_refs(schema: Value, renames: &Map<String, Value>) -> Value {
    match schema {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, val)| {
                    if key == "$ref" {
                        if let Some(name) = val
                            .as_str()
                            .and_then(|r| r.strip_prefix("#/components/schemas/"))
                            && let Some(renamed) = renames.get(name).and_then(Value::as_str)
                        {
                            return (key, json!(format!("#/components/schemas/{renamed}")));
                        }
                        (key, val)
                    } else {
                        (key, rename_refs(val, renames))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(|v| rename_refs(v, renames)).collect())
        }
        other => other,
    }
}

/// Build the OpenAPI document describing every REST route.
pub fn document() -> Value {
    let (mut schemas, renames) = merged_schemas();
    schemas.insert("ApiErrorBody".into(), api_error_schema());
    schemas.insert("OpenApiDocument".into(), open_api_document_schema());

    let paths: Map<String, Value> = ROUTES
        .iter()
        .map(|(path, ops)| ((*path).to_string(), path_item(path, ops, &renames)))
        .collect();

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "prosperod REST API",
            "version": env!("CARGO_PKG_VERSION"),
            "description":
                "The control plane for a fleet of caliban agents. Every route \
                 below except the open ones needs a scoped token \
                 (`Authorization: Bearer pspo_…`) or the dashboard session \
                 cookie; each operation records the scope it requires in \
                 `x-prospero-scope`.",
            // `identifier` is SPDX and is what OpenAPI 3.1 prefers over a URL.
            "license": { "name": "AGPL-3.0-only", "identifier": "AGPL-3.0-only" },
        },
        "servers": [{ "url": "/", "description": "This prosperod instance." }],
        "components": {
            "schemas": Value::Object(schemas),
            "securitySchemes": {
                "bearer": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": "A scoped prospero token (`pspo_…`).",
                },
                "session": {
                    "type": "apiKey",
                    "in": "cookie",
                    "name": "prospero_session",
                    "description": "The dashboard's sign-in cookie.",
                },
            },
        },
        "security": [{ "bearer": [] }, { "session": [] }],
        "paths": Value::Object(paths),
    })
}

/// Build one path item, deriving path parameters from the template.
fn path_item(path: &str, ops: &[Op], renames: &Map<String, Value>) -> Value {
    let mut item = Map::new();

    let params: Vec<Value> = path_params(path)
        .into_iter()
        .map(|name| {
            json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": { "type": "string" },
            })
        })
        .collect();
    if !params.is_empty() {
        item.insert("parameters".into(), Value::Array(params));
    }

    for op in ops {
        item.insert(op.method.to_string(), operation(op, renames));
    }
    Value::Object(item)
}

/// The `{name}` segments of a path template, in order.
///
/// Deriving these means a new parameterised route cannot ship with its
/// parameters undeclared.
fn path_params(path: &str) -> Vec<&str> {
    path.split('/')
        .filter_map(|seg| seg.strip_prefix('{')?.strip_suffix('}'))
        // `{*path}` is axum's catch-all syntax, not a named parameter.
        .filter(|name| !name.starts_with('*'))
        .collect()
}

fn operation(op: &Op, renames: &Map<String, Value>) -> Value {
    let mut out = Map::new();
    out.insert("operationId".into(), json!(op.id));
    out.insert("summary".into(), json!(op.summary));
    out.insert("x-prospero-scope".into(), json!(op.scope));

    if op.scope == "open" {
        // An open route takes no credential, so it must not inherit the
        // document-level security requirement.
        out.insert("security".into(), json!([]));
    }

    if !op.query.is_empty() {
        let params: Vec<Value> = op
            .query
            .iter()
            .map(|q| {
                json!({
                    "name": q.name,
                    "in": "query",
                    "required": false,
                    "description": q.description,
                    "schema": { "type": q.ty },
                })
            })
            .collect();
        out.insert("parameters".into(), Value::Array(params));
    }

    if let Some(schema) = op.request {
        // A request body refers to the type's read shape, which is published
        // under a `Request` name whenever it differs from the write shape.
        let published = renames
            .get(schema)
            .and_then(Value::as_str)
            .unwrap_or(schema);
        out.insert(
            "requestBody".into(),
            json!({
                "required": true,
                "content": { "application/json": { "schema": schema_ref(published) } },
            }),
        );
    }

    let mut responses = Map::new();
    for res in op.responses.iter().chain(ERRORS) {
        responses.insert(res.status.to_string(), response(res));
    }
    out.insert("responses".into(), Value::Object(responses));

    Value::Object(out)
}

fn response(res: &Res) -> Value {
    let content = match &res.body {
        Body::Empty => None,
        Body::Json(name) => Some(json!({ "application/json": { "schema": schema_ref(name) } })),
        Body::JsonArray(name) => Some(json!({
            "application/json": { "schema": { "type": "array", "items": schema_ref(name) } }
        })),
        Body::Text => Some(json!({ "text/plain": { "schema": { "type": "string" } } })),
        Body::Sse => Some(json!({ "text/event-stream": { "schema": { "type": "string" } } })),
    };

    match content {
        Some(content) => json!({ "description": res.description, "content": content }),
        None => json!({ "description": res.description }),
    }
}

fn schema_ref(name: &str) -> Value {
    json!({ "$ref": format!("#/components/schemas/{name}") })
}

/// The error envelope every failing route returns.
///
/// Hand-written because `ApiError` is an axum `IntoResponse`, not a serialized
/// struct — the shape below is what `error.rs` actually writes.
fn api_error_schema() -> Value {
    json!({
        "type": "object",
        "description": "The body of any 4xx or 5xx response.",
        "required": ["error", "kind"],
        "properties": {
            "error": { "type": "string", "description": "Human-readable message." },
            "kind": {
                "type": "string",
                "description":
                    "Machine-readable class, e.g. `unauthorized`, `not_found`, \
                     `invalid_state`, `unreachable`.",
            },
        },
    })
}

/// This document's own schema — deliberately open, since it is an OpenAPI
/// document and describing it in full would just be quoting the OpenAPI meta-schema.
fn open_api_document_schema() -> Value {
    json!({
        "type": "object",
        "description": "An OpenAPI 3.1 document.",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_parameters_come_from_the_template() {
        assert_eq!(path_params("/api/agents/{id}/events"), vec!["id"]);
        assert_eq!(
            path_params("/api/workspaces/{workspace}/agents"),
            vec!["workspace"]
        );
        assert!(path_params("/api/fleet").is_empty());
    }

    #[test]
    fn the_catch_all_asset_segment_is_not_a_parameter() {
        assert!(path_params("/assets/{*path}").is_empty());
    }

    /// A type whose read and write shapes differ must be published twice, or
    /// one of the two directions is being described by the other's schema.
    #[test]
    fn a_type_that_reads_and_writes_differently_is_published_twice() {
        let (schemas, renames) = merged_schemas();

        // `SpawnTemplate` is the concrete case: it is nested in a request body
        // (`CreateAutomationBody`) and in a response (`Automation`), and its
        // defaulted fields make the two shapes differ.
        assert!(
            renames.contains_key("SpawnTemplate"),
            "SpawnTemplate reads and writes differently, so it should have been \
             split; renames were {renames:#?}"
        );

        let write = schemas.get("SpawnTemplate").expect("response shape");
        let read = schemas
            .get("SpawnTemplateRequest")
            .expect("request shape should be published separately");
        assert_ne!(
            write, read,
            "the two shapes are published under different names but are identical, \
             so the split is pointless"
        );
    }

    /// Every type published under both names must be reachable under the right
    /// one: a request body must not quietly point at the response shape.
    #[test]
    fn request_bodies_refer_to_the_read_shape() {
        let doc = document();
        let body = &doc["paths"]["/api/automations"]["post"]["requestBody"]["content"]["application/json"]
            ["schema"]["$ref"];
        assert_eq!(body, "#/components/schemas/CreateAutomationBody");

        // …and that schema's nested template must be the read shape too.
        let nested = doc["components"]["schemas"]["CreateAutomationBody"]["properties"]["template"]
            ["$ref"]
            .as_str()
            .expect("template should be a $ref");
        assert_eq!(nested, "#/components/schemas/SpawnTemplateRequest");
    }
}
