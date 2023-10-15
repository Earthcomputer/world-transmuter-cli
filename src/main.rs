mod data;
mod dimensions;
mod indent;
mod individual_files;
mod region;

use crate::data::{upgrade_data, upgrade_map_data};
use crate::dimensions::upgrade_dimensions;
use crate::indent::Indent;
use crate::individual_files::{
    upgrade_advancements, upgrade_level_dat, upgrade_playerdata, upgrade_stats,
};
use clap::{arg, command, value_parser, ArgAction};
use std::path::PathBuf;
use std::sync::RwLockReadGuard;
use world_transmuter::types;
use world_transmuter::version_names::{get_version_by_id, get_version_by_name, VersionType};
use world_transmuter_engine::{AbstractMapDataType, JCompound, MapDataType};

const ADVANCEMENTS_AND_STATS_VERSION: u32 = 1343; // 1.12.2

fn main() {
    let indent = Indent::new();

    let _ = include_str!("../Cargo.toml"); // trick the compiler into recompiling when this changes
    let matches = command!()
        .arg(arg!(<world> "The path to the world folder").value_parser(value_parser!(PathBuf)))
        .arg(arg!(<to_version> "The version to update to"))
        .arg(arg!(-s --"allow-snapshots" ... "Allow snapshots").action(ArgAction::SetTrue))
        .arg(
            arg!(-d --"dry-run" ... "Don't write anything back to files")
                .action(ArgAction::SetTrue),
        )
        .get_matches();

    let world = matches.get_one::<PathBuf>("world").unwrap();

    let to_version = matches.get_one::<String>("to_version").unwrap();
    let Some(to_version) = get_version_by_name(to_version) else {
        println!("{indent}Unknown version {to_version}");
        return;
    };
    if to_version.typ == VersionType::Snapshot && !matches.get_flag("allow-snapshots") {
        println!(
            "{indent}{} is a snapshot. Use --allow-snapshots to upgrade the world anyway.",
            to_version.name
        );
        return;
    }
    let to_version = to_version.data_version;

    let dry_run = matches.get_flag("dry-run");

    let Some(level_dat) = upgrade_level_dat(indent, world, to_version, dry_run) else {
        return;
    };

    if to_version >= ADVANCEMENTS_AND_STATS_VERSION {
        upgrade_advancements(indent, world, to_version, dry_run);
        upgrade_stats(indent, world, to_version, dry_run);
    }

    upgrade_playerdata(indent, world, to_version, dry_run);

    upgrade_dimensions(indent, world, to_version, dry_run, &level_dat);

    upgrade_data(
        indent,
        world,
        "scoreboard",
        types::saved_data_scoreboard,
        to_version,
        dry_run,
    );
    upgrade_data(
        indent,
        world,
        "random_sequences",
        types::saved_data_random_sequences,
        to_version,
        dry_run,
    );
    upgrade_map_data(indent, world, to_version, dry_run);

    println!("{indent}Done");
}

#[must_use]
fn upgrade(
    indent: Indent,
    typ: impl FnOnce() -> RwLockReadGuard<'static, MapDataType<'static>>,
    data: &mut JCompound,
    name: impl FnOnce() -> String,
    to_version: u32,
    default_version: u32,
) -> bool {
    let from_version = data
        .remove("DataVersion")
        .and_then(|v| v.as_i32())
        .map(|v| v as u32)
        .unwrap_or(default_version);
    let Some(from_version) = get_version_by_id(from_version) else {
        println!(
            "{indent}{} had unrecognized data version {}",
            name(),
            from_version
        );
        return false;
    };

    if from_version.data_version > to_version {
        println!(
            "{indent}Cannot downgrade {} from {}",
            name(),
            from_version.name
        );
        return false;
    }

    typ().convert(data, from_version.data_version.into(), to_version.into());
    data.insert("DataVersion", to_version as i32);

    true
}
