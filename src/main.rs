mod data;
mod dimensions;
mod individual_files;
mod region;

use crate::data::{upgrade_data, upgrade_map_data};
use crate::dimensions::upgrade_dimensions;
use crate::individual_files::{
    upgrade_advancements, upgrade_level_dat, upgrade_playerdata, upgrade_stats,
};
use clap::{arg, command, value_parser, ArgAction};
use std::fmt::Write;
use std::path::PathBuf;
use std::sync::RwLockReadGuard;
use time::OffsetDateTime;
use tracing::{error, info, warn, Level};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};
use tracing_tree::time::FormatTime;
use tracing_tree::HierarchicalLayer;
use world_transmuter::types;
use world_transmuter::version_names::{get_version_by_id, get_version_by_name, VersionType};
use world_transmuter_engine::{AbstractMapDataType, JCompound, MapDataType};

const ADVANCEMENTS_AND_STATS_VERSION: u32 = 1343; // 1.12.2

fn main() {
    struct MyFormatTime;
    impl FormatTime for MyFormatTime {
        fn format_time(&self, w: &mut impl Write) -> std::fmt::Result {
            #[cfg(target_family = "unix")]
            {
                let time = OffsetDateTime::now_utc();
                write!(
                    w,
                    "{} {:02}:{:02}:{:02}",
                    time.date(),
                    time.hour(),
                    time.minute(),
                    time.second()
                )
            }
            #[cfg(not(target_family = "unix"))]
            {
                let time = OffsetDateTime::now_local().expect("time offset cannot be determined");
                write!(
                    w,
                    "{} {:02}:{:02}:{:02} {}",
                    time.date(),
                    time.hour(),
                    time.minute(),
                    time.second(),
                    time.offset()
                )
            }
        }
    }
    Registry::default()
        .with(
            HierarchicalLayer::new(2)
                .with_timer(MyFormatTime)
                .with_higher_precision(false),
        )
        .with(
            EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

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
        error!("Unknown version {to_version}");
        return;
    };
    if to_version.typ == VersionType::Snapshot && !matches.get_flag("allow-snapshots") {
        error!(
            "{} is a snapshot. Use --allow-snapshots to upgrade the world anyway.",
            to_version.name
        );
        return;
    }
    let to_version = to_version.data_version;

    let dry_run = matches.get_flag("dry-run");

    let Some(level_dat) = upgrade_level_dat(world, to_version, dry_run) else {
        return;
    };

    if to_version >= ADVANCEMENTS_AND_STATS_VERSION {
        upgrade_advancements(world, to_version, dry_run);
        upgrade_stats(world, to_version, dry_run);
    }

    upgrade_playerdata(world, to_version, dry_run);

    upgrade_dimensions(world, to_version, dry_run, &level_dat);

    upgrade_data(
        world,
        "scoreboard",
        types::saved_data_scoreboard,
        to_version,
        dry_run,
    );
    upgrade_data(
        world,
        "random_sequences",
        types::saved_data_random_sequences,
        to_version,
        dry_run,
    );
    upgrade_map_data(world, to_version, dry_run);

    info!("Done");
}

#[must_use]
fn upgrade(
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
        warn!("{} had unrecognized data version {}", name(), from_version);
        return false;
    };

    if from_version.data_version > to_version {
        warn!("Cannot downgrade {} from {}", name(), from_version.name);
        return false;
    }

    typ().convert(data, from_version.data_version.into(), to_version.into());
    data.insert("DataVersion", to_version as i32);

    true
}
