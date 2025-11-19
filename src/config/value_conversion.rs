use toml::Value as TomlValue;

/// TOML値から型への変換トレイト
pub trait FromTomlValue: Sized {
    fn from_toml_value(value: &TomlValue) -> Option<Self>;
}

impl FromTomlValue for bool {
    fn from_toml_value(value: &TomlValue) -> Option<Self> {
        match value {
            TomlValue::Boolean(b) => Some(*b),
            TomlValue::String(s) => Some(parse_bool(s)),
            _ => None,
        }
    }
}

impl FromTomlValue for String {
    fn from_toml_value(value: &TomlValue) -> Option<Self> {
        value.as_str().map(|s| s.to_string())
    }
}

impl FromTomlValue for Vec<String> {
    fn from_toml_value(value: &TomlValue) -> Option<Self> {
        value.as_array().map(|arr| {
            arr.iter()
                .filter_map(|item| item.as_str().map(|s| s.to_string()))
                .collect()
        })
    }
}

impl FromTomlValue for Option<String> {
    fn from_toml_value(value: &TomlValue) -> Option<Self> {
        Some(value.as_str().map(|s| s.to_string()))
    }
}

impl FromTomlValue for u32 {
    fn from_toml_value(value: &TomlValue) -> Option<Self> {
        match value {
            TomlValue::Integer(i) if *i >= 0 => Some(*i as u32),
            TomlValue::String(s) => s.trim().parse::<u32>().ok(),
            _ => None,
        }
    }
}

/// 文字列をboolに変換
/// "true", "1", "yes", "on" などは true、それ以外は false
fn parse_bool(s: &str) -> bool {
    let s_lower = s.to_lowercase();
    let s_trimmed = s_lower.trim();
    matches!(s_trimmed, "true" | "1" | "yes" | "on" | "y")
}
