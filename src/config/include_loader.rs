use crate::config::expansion::ExpandContext;
use crate::config::SystemConfig;
use anyhow::Result;
use std::collections::HashSet;
use std::path::Path;

/// includeファイルを読み込んでSystemConfigに統合
pub(crate) fn load_includes(config: &mut SystemConfig, base_config_path: &str) -> Result<()> {
    let mut loaded_files = HashSet::new();
    let base_dir = Path::new(base_config_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let include_paths = config.include_paths.clone();
    load_includes_recursive(config, &mut loaded_files, base_dir, &include_paths)?;
    Ok(())
}

fn load_includes_recursive(
    config: &mut SystemConfig,
    loaded_files: &mut HashSet<String>,
    base_dir: &Path,
    include_paths: &[String],
) -> Result<()> {
    for include_path in include_paths {
        let full_path = if Path::new(include_path).is_absolute() {
            include_path.clone()
        } else {
            // Standard approach: resolve relative to the config file's directory
            // This is the most common pattern used by Docker Compose, Nginx, etc.
            base_dir.join(include_path).to_string_lossy().to_string()
        };

        if loaded_files.contains(&full_path) {
            log::warn!("Circular include detected for file: {}", full_path);
            continue;
        }

        if !Path::new(&full_path).exists() {
            log::warn!(
                "Include file not found: {} (base_dir: {}, include_path: {})",
                full_path,
                base_dir.display(),
                include_path
            );
            continue;
        }

        match std::fs::read_to_string(&full_path) {
            Ok(content) => {
                loaded_files.insert(full_path.clone());
                log::info!("Loading include file: {}", full_path);

                // includeファイル用の展開コンテキストを作成
                let include_expand_context = ExpandContext::from_config_path(&full_path);

                match SystemConfig::from_toml_internal(&content, false, &include_expand_context) {
                    Ok(included_config) => {
                        // from_toml_internal already calls normalize_defaults()
                        log::info!(
                            "Loaded {} tasks from {}",
                            included_config.tasks.len(),
                            full_path
                        );
                        for (name, task) in included_config.tasks {
                            if config.tasks.insert(name.clone(), task).is_some() {
                                log::warn!(
                                    "Duplicate task name '{}' in include file {}, overwriting",
                                    name,
                                    full_path
                                );
                            }
                        }
                        if !included_config.include_paths.is_empty() {
                            let include_dir = Path::new(&full_path)
                                .parent()
                                .unwrap_or_else(|| Path::new("."));

                            load_includes_recursive(
                                config,
                                loaded_files,
                                include_dir,
                                &included_config.include_paths,
                            )?;
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to parse include file {}: {}", full_path, e);
                    }
                }
            }
            Err(e) => {
                log::error!("Failed to read include file {}: {}", full_path, e);
            }
        }
    }

    Ok(())
}
