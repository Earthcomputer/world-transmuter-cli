use crate::{upgrade, ADVANCEMENTS_AND_STATS_VERSION};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use java_string::JavaStr;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use std::sync::RwLockReadGuard;
use tracing::{error, info_span, warn, Span};
use valence_nbt::{from_binary, to_binary};
use world_transmuter::json::{parse_compound, stringify_compound};
use world_transmuter::types;
use world_transmuter::version_names::{get_version_by_id, get_versions};
use world_transmuter_engine::{AbstractMapDataType, JCompound, JValue, MapDataType};

const OLD_SETTINGS_KEYS: [&str; 7] = [
    "RandomSeed",
    "generatorName",
    "generatorOptions",
    "generatorVersion",
    "legacy_custom_options",
    "MapFeatures",
    "BonusChest",
];

pub fn upgrade_level_dat(world: &Path, to_version: u32, dry_run: bool) -> Option<JCompound> {
    let _span = info_span!("Upgrading level.dat").entered();
    fn update_data(data: &mut JCompound, from_version: u32, to_version: u32) {
        data.remove("Player"); // TODO: what is this?

        types::level().convert(data, from_version.into(), to_version.into());

        data.insert("DataVersion", to_version as i32);

        if to_version >= 2554 {
            // 20w21a
            let old_settings: Vec<_> = OLD_SETTINGS_KEYS
                .iter()
                .copied()
                .filter_map(|old_settings_key| {
                    data.remove(old_settings_key)
                        .map(|value| (old_settings_key, value))
                })
                .collect();
            if !matches!(data.get("WorldGenSettings"), Some(JValue::Compound(_))) {
                data.insert("WorldGenSettings", JCompound::new());
            }
            let Some(JValue::Compound(world_gen_settings)) = data.get_mut("WorldGenSettings")
            else {
                unreachable!();
            };
            for (key, value) in old_settings {
                world_gen_settings.insert(key, value);
            }
            types::world_gen_settings().convert(
                world_gen_settings,
                from_version.into(),
                to_version.into(),
            );
        }
    }

    let path = world.join("level.dat");
    let Ok(mut file) = File::options().read(true).write(!dry_run).open(&path) else {
        error!("Failed to open {}", path.to_string_lossy());
        return None;
    };

    let Some(mut level_dat) = read_compound(&mut file) else {
        error!("Failed to read level.dat");
        return None;
    };

    let Some(JValue::Compound(data)) = level_dat.get_mut("Data") else {
        error!("Missing Data tag in level.dat");
        return None;
    };

    let latest_version = get_versions().next_back().unwrap().data_version;
    let data_version = data
        .remove("DataVersion")
        .and_then(|v| v.as_i32())
        .unwrap_or(99) as u32;
    let Some(data_version) = get_version_by_id(data_version) else {
        warn!("level.dat had unrecognized data version {data_version}");
        return None;
    };
    if data_version.data_version > to_version {
        warn!("Cannot downgrade level.dat from {}", data_version.name);

        update_data(data, data_version.data_version, latest_version);

        let Some(JValue::Compound(data)) = level_dat.remove("Data") else {
            unreachable!()
        };
        return Some(data);
    }

    update_data(data, data_version.data_version, to_version);

    if !dry_run && !write_compound(file, &level_dat) {
        error!("Failed to write back to level.dat");
        return None;
    }

    let Some(JValue::Compound(mut data)) = level_dat.remove("Data") else {
        unreachable!()
    };

    update_data(&mut data, to_version, latest_version);

    Some(data)
}

pub fn upgrade_playerdata(world: &Path, to_version: u32, dry_run: bool) {
    upgrade_dat_dir(world, to_version, dry_run, "playerdata", types::player);
}

fn upgrade_dat_dir(
    world: &Path,
    to_version: u32,
    dry_run: bool,
    name: &str,
    typ: impl Sync + Send + Fn() -> RwLockReadGuard<'static, MapDataType<'static>>,
) {
    let _span = info_span!("Upgrading data directory", message = name).entered();
    let dat_dir = world.join(name);
    match std::fs::read_dir(dat_dir) {
        Ok(dir) => {
            let parent_span = Span::current();
            dir.collect::<Vec<_>>().into_par_iter().for_each_init(
                move || parent_span.clone().entered(),
                |_, file| match file {
                    Ok(file) => {
                        let path = file.path();
                        if path.extension() == Some("dat".as_ref()) {
                            let mut file = match File::options()
                                .read(true)
                                .write(!dry_run)
                                .open(&path)
                            {
                                Ok(file) => file,
                                Err(err) => {
                                    error!("Failed to open {}: {}", path.to_string_lossy(), err);
                                    return;
                                }
                            };
                            let Some(mut data) = read_compound(&mut file) else {
                                error!("Failed to read {}", path.to_string_lossy());
                                return;
                            };

                            if !upgrade(
                                &typ,
                                &mut data,
                                || path.to_string_lossy().into_owned(),
                                to_version,
                                99,
                            ) {
                                return;
                            }

                            if !dry_run && !write_compound(&mut file, &data) {
                                error!("Failed to write file {}", path.to_string_lossy());
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to read {name} directory entry: {err}");
                    }
                },
            );
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            error!("Failed to read {name} dir: {err}");
        }
    }
}

pub fn upgrade_advancements(world: &Path, to_version: u32, dry_run: bool) {
    upgrade_json_dir(
        world,
        to_version,
        dry_run,
        "advancements",
        true,
        types::advancements,
    )
}

pub fn upgrade_stats(world: &Path, to_version: u32, dry_run: bool) {
    upgrade_json_dir(world, to_version, dry_run, "stats", false, types::stats);
}

fn upgrade_json_dir(
    world: &Path,
    to_version: u32,
    dry_run: bool,
    name: &str,
    pretty_json: bool,
    typ: impl Sync + Send + Fn() -> RwLockReadGuard<'static, MapDataType<'static>>,
) {
    let _span = info_span!("Upgrading json directory", message = name).entered();
    let json_dir = world.join(name);
    match std::fs::read_dir(json_dir) {
        Ok(dir) => {
            let parent_span = Span::current();
            dir.collect::<Vec<_>>().into_par_iter().for_each_init(
                move || parent_span.clone().entered(),
                |_, file| match file {
                    Ok(file) => {
                        let path = file.path();
                        if path.extension() == Some("json".as_ref()) {
                            let json = match std::fs::read_to_string(&path) {
                                Ok(json) => json,
                                Err(err) => {
                                    error!("Failed to read {}: {}", path.to_string_lossy(), err);
                                    return;
                                }
                            };
                            let mut compound = match parse_compound(JavaStr::from_str(&json), true)
                            {
                                Ok(compound) => compound,
                                Err(err) => {
                                    error!("Failed to read {}: {}", path.to_string_lossy(), err);
                                    return;
                                }
                            };

                            if !upgrade(
                                &typ,
                                &mut compound,
                                || path.to_string_lossy().into_owned(),
                                to_version,
                                ADVANCEMENTS_AND_STATS_VERSION,
                            ) {
                                return;
                            }

                            if !dry_run {
                                if let Err(err) = std::fs::write(
                                    &path,
                                    stringify_compound(compound, true, pretty_json),
                                ) {
                                    error!(
                                        "Failed to write file {}: {}",
                                        path.to_string_lossy(),
                                        err
                                    );
                                }
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to read {name} directory entry: {err}");
                    }
                },
            );
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            error!("Failed to read {name} dir: {err}");
        }
    }
}

pub fn read_compound<R: Read>(read: R) -> Option<JCompound> {
    let mut contents = Vec::new();
    if GzDecoder::new(read).read_to_end(&mut contents).is_err() {
        return None;
    }
    from_binary(&mut &*contents)
        .ok()
        .map(|(compound, _)| compound)
}

#[must_use]
fn write_compound<W: Write>(write: W, data: &JCompound) -> bool {
    to_binary(data, GzEncoder::new(write, Compression::default()), "").is_ok()
}
