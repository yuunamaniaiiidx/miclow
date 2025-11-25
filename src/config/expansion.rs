use anyhow::{Context, Result};
use std::collections::HashMap;
use std::env;

#[derive(Debug, Clone)]
pub struct ExpandContext {
    virtual_env: HashMap<String, String>,
}

impl ExpandContext {
    pub fn new() -> Self {
        Self {
            virtual_env: HashMap::new(),
        }
    }

    pub fn from_config_path(config_path: &str) -> Self {
        let mut context = Self::new();

        let config_dir = std::path::Path::new(config_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| ".".to_string());
        context.set_virtual_env("MICLOW_CONFIG_DIR", &config_dir);

        context
    }

    pub fn set_virtual_env(&mut self, key: &str, value: &str) {
        self.virtual_env.insert(key.to_string(), value.to_string());
    }

    fn get_var(&self, var_name: &str) -> Option<String> {
        if let Some(value) = self.virtual_env.get(var_name) {
            return Some(value.clone());
        }
        env::var(var_name).ok()
    }
}

impl Default for ExpandContext {
    fn default() -> Self {
        Self::new()
    }
}

pub trait Expandable {
    fn expand(&self, context: &ExpandContext) -> Result<Self>
    where
        Self: Sized;
}

impl Expandable for String {
    fn expand(&self, context: &ExpandContext) -> Result<Self> {
        expand_variables(self, context)
    }
}

impl Expandable for Vec<String> {
    fn expand(&self, context: &ExpandContext) -> Result<Self> {
        self.iter().map(|item| item.expand(context)).collect()
    }
}

impl<T: Expandable> Expandable for Option<T> {
    fn expand(&self, context: &ExpandContext) -> Result<Self> {
        self.as_ref().map(|value| value.expand(context)).transpose()
    }
}

impl Expandable for HashMap<String, String> {
    fn expand(&self, context: &ExpandContext) -> Result<Self> {
        let mut expanded = HashMap::new();
        for (key, value) in self.iter() {
            let expanded_key = key.expand(context)?;
            let expanded_value = value.expand(context)?;
            expanded.insert(expanded_key, expanded_value);
        }
        Ok(expanded)
    }
}

pub fn expand_variables(value: &str, context: &ExpandContext) -> Result<String> {
    use regex::Regex;

    let re = Regex::new(r"\$\{([^}]+)\}").context("Failed to compile regex pattern")?;

    let mut result = value.to_string();
    let mut replacements = Vec::new();

    for cap in re.captures_iter(value) {
        let full_match = cap.get(0).unwrap();
        let var_expr = cap.get(1).unwrap().as_str();

        let (var_name, default_value, treat_empty_as_unset) =
            if let Some(colon_dash_pos) = var_expr.find(":-") {
                let name = &var_expr[..colon_dash_pos];
                let default = &var_expr[colon_dash_pos + 2..];
                (name, Some(default), true)
            } else if let Some(dash_pos) = var_expr.find('-') {
                let name = &var_expr[..dash_pos];
                let default = &var_expr[dash_pos + 1..];
                (name, Some(default), false)
            } else {
                (var_expr, None, false)
            };

        let var_value = context.get_var(var_name);

        let replacement = match var_value {
            Some(value) if !value.is_empty() => value,
            Some(value) => {
                if treat_empty_as_unset {
                    if let Some(default) = default_value {
                        default.to_string()
                    } else {
                        return Err(anyhow::anyhow!(
                            "Variable '{}' is empty and no default value provided",
                            var_name
                        ));
                    }
                } else {
                    value
                }
            }
            None => {
                if let Some(default) = default_value {
                    default.to_string()
                } else {
                    return Err(anyhow::anyhow!(
                        "Variable '{}' is not set and no default value provided",
                        var_name
                    ));
                }
            }
        };

        replacements.push((full_match.start(), full_match.end(), replacement));
    }

    for (start, end, replacement) in replacements.into_iter().rev() {
        result.replace_range(start..end, &replacement);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_expand_simple_var() {
        env::set_var("TEST_VAR", "test_value");
        let context = ExpandContext::new();
        let result = expand_variables("${TEST_VAR}", &context).unwrap();
        assert_eq!(result, "test_value");
        env::remove_var("TEST_VAR");
    }

    #[test]
    fn test_expand_var_with_default() {
        let context = ExpandContext::new();
        let result = expand_variables("${UNDEFINED_VAR-default_value}", &context).unwrap();
        assert_eq!(result, "default_value");
    }

    #[test]
    fn test_expand_var_with_default_when_defined() {
        env::set_var("DEFINED_VAR", "actual_value");
        let context = ExpandContext::new();
        let result = expand_variables("${DEFINED_VAR-default_value}", &context).unwrap();
        assert_eq!(result, "actual_value");
        env::remove_var("DEFINED_VAR");
    }

    #[test]
    fn test_expand_undefined_var_error() {
        let context = ExpandContext::new();
        let result = expand_variables("${UNDEFINED_VAR}", &context);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not set"));
    }

    #[test]
    fn test_expand_virtual_env() {
        let mut context = ExpandContext::new();
        context.set_virtual_env("TEST_VAR", "test_value");
        let result = expand_variables("${TEST_VAR}", &context).unwrap();
        assert_eq!(result, "test_value");
    }

    #[test]
    fn test_expand_virtual_env_with_default() {
        let context = ExpandContext::new();
        let result = expand_variables("${UNDEFINED_VAR-/default/path}", &context).unwrap();
        assert_eq!(result, "/default/path");
    }

    #[test]
    fn test_expand_from_config_path() {
        let context = ExpandContext::from_config_path("/home/user/config.toml");
        let result = expand_variables("${MICLOW_CONFIG_DIR}", &context).unwrap();
        assert_eq!(result, "/home/user");
    }

    #[test]
    fn test_expand_multiple_vars() {
        env::set_var("VAR1", "value1");
        env::set_var("VAR2", "value2");
        let context = ExpandContext::new();
        let result = expand_variables("${VAR1} and ${VAR2}", &context).unwrap();
        assert_eq!(result, "value1 and value2");
        env::remove_var("VAR1");
        env::remove_var("VAR2");
    }

    #[test]
    fn test_expand_mixed_vars() {
        env::set_var("ENV_VAR", "env_value");
        let mut context = ExpandContext::new();
        context.set_virtual_env("virtual_var", "virtual_value");
        let result = expand_variables("${ENV_VAR} and ${virtual_var}", &context).unwrap();
        assert_eq!(result, "env_value and virtual_value");
        env::remove_var("ENV_VAR");
    }

    #[test]
    fn test_expand_no_vars() {
        let context = ExpandContext::new();
        let result = expand_variables("no variables here", &context).unwrap();
        assert_eq!(result, "no variables here");
    }

    #[test]
    fn test_expand_empty_string() {
        let context = ExpandContext::new();
        let result = expand_variables("", &context).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_expand_default_with_dash_in_value() {
        let context = ExpandContext::new();
        let result = expand_variables("${VAR-default-value}", &context).unwrap();
        assert_eq!(result, "default-value");
    }

    #[test]
    fn test_expand_virtual_overrides_env() {
        env::set_var("TEST_VAR", "env_value");
        let mut context = ExpandContext::new();
        context.set_virtual_env("TEST_VAR", "virtual_value");
        let result = expand_variables("${TEST_VAR}", &context).unwrap();
        assert_eq!(result, "virtual_value");
        env::remove_var("TEST_VAR");
    }

    #[test]
    fn test_expand_colon_dash_default() {
        // ${VAR:-default} 形式: 未定義または空文字列の場合にデフォルト値を使用
        let context = ExpandContext::new();
        let result = expand_variables("${UNDEFINED_VAR:-default_value}", &context).unwrap();
        assert_eq!(result, "default_value");
    }

    #[test]
    fn test_expand_colon_dash_default_when_defined() {
        // ${VAR:-default} 形式: 変数が定義されている場合はその値を使用
        env::set_var("DEFINED_VAR", "actual_value");
        let context = ExpandContext::new();
        let result = expand_variables("${DEFINED_VAR:-default_value}", &context).unwrap();
        assert_eq!(result, "actual_value");
        env::remove_var("DEFINED_VAR");
    }

    #[test]
    fn test_expand_colon_dash_default_with_empty_string() {
        // ${VAR:-default} 形式: 変数が空文字列の場合はデフォルト値を使用
        env::set_var("EMPTY_VAR", "");
        let context = ExpandContext::new();
        let result = expand_variables("${EMPTY_VAR:-default_value}", &context).unwrap();
        assert_eq!(result, "default_value");
        env::remove_var("EMPTY_VAR");
    }

    #[test]
    fn test_expand_dash_default_with_empty_string() {
        // ${VAR-default} 形式: 変数が空文字列の場合は空文字列を有効な値として扱う
        // 他のテストとの干渉を避けるため、一意な変数名を使用
        let var_name = "EMPTY_VAR_DASH_TEST";
        env::remove_var(var_name);
        env::set_var(var_name, "");
        let context = ExpandContext::new();
        let result =
            expand_variables(&format!("${{{}-default_value}}", var_name), &context).unwrap();
        assert_eq!(result, "", "空文字列は有効な値として扱われるべき");
        env::remove_var(var_name);
    }

    #[test]
    fn test_expand_colon_dash_with_dash_in_default() {
        // ${VAR:-default-value} 形式: デフォルト値に - を含められる
        let context = ExpandContext::new();
        let result = expand_variables("${VAR:-http://example.com}", &context).unwrap();
        assert_eq!(result, "http://example.com");
    }

    #[test]
    fn test_expand_var_name_with_dash() {
        // 変数名に - を含む場合の動作確認
        // ${BASE-VAR} は ${BASE} と解釈され、-VAR がデフォルト値として扱われる（Docker Composeの仕様）

        // 一意な変数名を使用して他のテストとの干渉を避ける
        // ランダムな数値を使用して環境変数の衝突を避ける
        let base_var_name = "BASEXYZ999DASHTEST";
        // 環境変数を確実に削除
        let _ = env::var(base_var_name);
        env::remove_var(base_var_name);
        let dash_var_name = format!("{}-VAR", base_var_name);
        env::remove_var(&dash_var_name);

        // 環境変数が存在しないことを確認
        assert!(
            env::var(base_var_name).is_err(),
            "{} は存在しないべき",
            base_var_name
        );

        let context = ExpandContext::new();
        // ${BASEXYZ999DASHTEST-VAR} は ${BASEXYZ999DASHTEST} と解釈され、-VAR がデフォルト値として扱われる
        // BASEXYZ999DASHTEST が未定義の場合、デフォルト値 "VAR" が使われる
        let pattern = format!("${{{}-VAR}}", base_var_name);
        let result = expand_variables(&pattern, &context).unwrap();
        assert_eq!(
            result, "VAR",
            "{} が未定義の場合、デフォルト値 VAR が使われるべき",
            base_var_name
        );

        // 変数名に - を含む場合は :- を使う必要がある
        // ${BASEXYZ999DASHTEST-VAR:-default} では、:- の前まで（BASEXYZ999DASHTEST-VAR）が変数名として解釈される
        env::set_var(&dash_var_name, "dash-value");
        let context = ExpandContext::new();
        let pattern = format!("${{{}-VAR:-default}}", base_var_name);
        let result = expand_variables(&pattern, &context).unwrap();
        assert_eq!(result, "dash-value");
        env::remove_var(&dash_var_name);

        // ${BASEXYZ999DASHTEST-VAR-default} では、最初の - が区切りとして扱われるため、
        // BASEXYZ999DASHTEST という変数名として解釈され、-VAR-default がデフォルト値として扱われる
        env::remove_var(base_var_name);
        env::set_var(base_var_name, "dash-value");
        let context = ExpandContext::new();
        let pattern = format!("${{{}-VAR-default}}", base_var_name);
        let result = expand_variables(&pattern, &context).unwrap();
        assert_eq!(result, "dash-value"); // BASEXYZ999DASHTEST の値が使われる
        env::remove_var(base_var_name);
    }

    #[test]
    fn test_expand_colon_dash_vs_dash_difference() {
        // :- と - の違いを確認
        // 他のテストとの干渉を避けるため、一意な変数名を使用
        let var_name = "EMPTY_VAR_DIFF_TEST";
        env::remove_var(var_name);
        env::set_var(var_name, "");

        let context = ExpandContext::new();

        // ${VAR:-default} は空文字列を未定義として扱う
        let pattern1 = format!("${{{}:-default}}", var_name);
        let result1 = expand_variables(&pattern1, &context).unwrap();
        assert_eq!(result1, "default");

        // ${VAR-default} は空文字列を有効な値として扱う
        let pattern2 = format!("${{{}-default}}", var_name);
        let result2 = expand_variables(&pattern2, &context).unwrap();
        assert_eq!(result2, "");

        env::remove_var(var_name);
    }
}
