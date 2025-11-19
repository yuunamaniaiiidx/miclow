use crate::backend::{
    get_default_view_stderr, get_default_view_stdout, get_force_allow_duplicate,
    get_force_auto_start,
};
use crate::config::RawTaskConfig;
use anyhow::Result;
use toml::Value as TomlValue;

impl RawTaskConfig {
    /// 強制値とデフォルト値を設定（バリデーションも含む）
    pub(crate) fn normalize_defaults(&mut self, backend_name: &str) -> Result<()> {
        // 強制値を取得
        let force_allow_duplicate =
            get_force_allow_duplicate(backend_name).map_err(|e| anyhow::anyhow!("{}", e))?;
        let force_auto_start =
            get_force_auto_start(backend_name).map_err(|e| anyhow::anyhow!("{}", e))?;

        // タスク名を取得（エラーメッセージ用）
        let task_name = self.name.as_str().unwrap_or("unknown");

        // バリデーション: Interactiveバックエンドの場合、ユーザーが設定していたらエラー
        if backend_name == "Interactive" {
            if self.allow_duplicate.is_some() {
                return Err(anyhow::anyhow!(
                    "Task '{}' (protocol: {}) cannot have 'allow_duplicate' set by user. It is forced to {} by the backend implementation.",
                    task_name,
                    backend_name,
                    force_allow_duplicate
                ));
            }
            if self.auto_start.is_some() {
                return Err(anyhow::anyhow!(
                    "Task '{}' (protocol: {}) cannot have 'auto_start' set by user. It is forced to {} by the backend implementation.",
                    task_name,
                    backend_name,
                    force_auto_start
                ));
            }
        }

        // 強制値を設定
        if self.allow_duplicate.is_none() {
            self.allow_duplicate = Some(TomlValue::Boolean(force_allow_duplicate));
        }
        if self.auto_start.is_none() {
            self.auto_start = Some(TomlValue::Boolean(force_auto_start));
        }

        // デフォルト値を設定
        if self.view_stdout.is_none() {
            let default_value =
                get_default_view_stdout(backend_name).map_err(|e| anyhow::anyhow!("{}", e))?;
            self.view_stdout = Some(TomlValue::Boolean(default_value));
        }
        if self.view_stderr.is_none() {
            let default_value =
                get_default_view_stderr(backend_name).map_err(|e| anyhow::anyhow!("{}", e))?;
            self.view_stderr = Some(TomlValue::Boolean(default_value));
        }

        Ok(())
    }
}
