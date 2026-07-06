//! Prospero's normalized event type — the stable contract consumers see.
//!
//! The types now live in [`prospero_types`] (so the WASM dashboard can share
//! them, prospero #98) and are re-exported here from their original path.
//! Caliban's raw stream-json frames are normalized into [`FleetEvent`]s by
//! [`crate::caliband::stream::normalize_frame`]; consumers (CLI `follow`,
//! dashboard SSE, history replay) never see raw caliban frames.

pub use prospero_types::{EventKind, FleetEvent, OutputStream, stream_key_for};
