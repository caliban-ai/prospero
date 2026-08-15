//! The operator's theme preference.
//!
//! v2 ships a designed light theme and a designed dark one, but until now which
//! you got was decided entirely by the OS. This makes it a choice: follow the
//! system, or pin one.
//!
//! The resolution rules are plain data so they are testable on the host target;
//! `ui.rs` only reads and writes `localStorage` and stamps the root element.

/// What the operator picked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Theme {
    /// Follow `prefers-color-scheme`. The default, and the behaviour before
    /// this setting existed.
    #[default]
    System,
    /// Always light, whatever the OS says.
    Light,
    /// Always dark, whatever the OS says.
    Dark,
}

/// The key the preference is stored under.
pub const STORAGE_KEY: &str = "prospero.theme";

impl Theme {
    /// The value written to storage and to the root element's `data-theme`.
    ///
    /// `System` maps to `None`: the attribute is *removed* rather than set to
    /// a "system" string, so the stylesheet's `prefers-color-scheme` query is
    /// what applies. Encoding it as an attribute value would mean the CSS had
    /// to special-case a third state.
    pub fn attribute(self) -> Option<&'static str> {
        match self {
            Theme::System => None,
            Theme::Light => Some("light"),
            Theme::Dark => Some("dark"),
        }
    }

    /// The token persisted in `localStorage`.
    pub fn as_str(self) -> &'static str {
        match self {
            Theme::System => "system",
            Theme::Light => "light",
            Theme::Dark => "dark",
        }
    }

    /// Parse a stored value.
    ///
    /// Anything unrecognised — a corrupted entry, a value from a future
    /// version, a user poking at devtools — falls back to `System` rather than
    /// erroring. A bad preference should never be able to break the page.
    pub fn parse(stored: Option<&str>) -> Self {
        match stored.map(str::trim) {
            Some("light") => Theme::Light,
            Some("dark") => Theme::Dark,
            _ => Theme::System,
        }
    }

    /// Human-facing label for the control.
    pub fn label(self) -> &'static str {
        match self {
            Theme::System => "System",
            Theme::Light => "Light",
            Theme::Dark => "Dark",
        }
    }

    /// The order the control cycles through.
    pub fn next(self) -> Self {
        match self {
            Theme::System => Theme::Light,
            Theme::Light => Theme::Dark,
            Theme::Dark => Theme::System,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every option. A test helper rather than a public API: the control cycles
    /// with `next()` and nothing in the app needs to enumerate them.
    fn all() -> [Theme; 3] {
        [Theme::System, Theme::Light, Theme::Dark]
    }

    #[test]
    fn system_removes_the_attribute_so_the_media_query_applies() {
        assert_eq!(Theme::System.attribute(), None);
        assert_eq!(Theme::Light.attribute(), Some("light"));
        assert_eq!(Theme::Dark.attribute(), Some("dark"));
    }

    #[test]
    fn a_stored_value_round_trips() {
        for theme in all() {
            assert_eq!(Theme::parse(Some(theme.as_str())), theme);
        }
    }

    /// A corrupt preference must never break the page.
    #[test]
    fn anything_unrecognised_falls_back_to_system() {
        for bad in [
            None,
            Some(""),
            Some("  "),
            Some("nonsense"),
            Some("DARK"),
            Some("null"),
        ] {
            assert_eq!(Theme::parse(bad), Theme::System, "input was {bad:?}");
        }
    }

    #[test]
    fn stored_values_tolerate_surrounding_whitespace() {
        assert_eq!(Theme::parse(Some(" dark ")), Theme::Dark);
        assert_eq!(Theme::parse(Some("\tlight\n")), Theme::Light);
    }

    #[test]
    fn cycling_visits_every_option_and_returns_to_start() {
        let mut seen = Vec::new();
        let mut t = Theme::System;
        for _ in 0..3 {
            seen.push(t);
            t = t.next();
        }
        assert_eq!(t, Theme::System, "cycle should return to where it started");
        assert_eq!(seen.len(), 3);
        for option in all() {
            assert!(
                seen.contains(&option),
                "{option:?} is unreachable by cycling"
            );
        }
    }

    #[test]
    fn every_option_has_a_label() {
        for theme in all() {
            assert!(!theme.label().is_empty());
        }
    }

    #[test]
    fn the_default_follows_the_system() {
        assert_eq!(Theme::default(), Theme::System);
    }
}
