use std::io::ErrorKind;
use crate::data::upgrade_data;
use crate::region::{delete_legacy_dat_files, upgrade_chunks, upgrade_entities, upgrade_poi};
use java_string::JavaStr;
use std::path::Path;
use tracing::{error, info_span};
use world_transmuter::types;
use world_transmuter_engine::{JCompound, JValue};

const FIRST_RAIDS_VERSION: u32 = 1912; // 18w47a
const NETHER_RAIDS_RENAME: u32 = 2972; // 1.18.2-pre2

fn get_custom_dimensions(level_dat: &JCompound) -> Vec<(&JavaStr, &JavaStr, &JavaStr)> {
    let Some(JValue::Compound(world_gen_settings)) = level_dat.get("WorldGenSettings") else {
        return Vec::new();
    };
    let Some(JValue::Compound(dimensions)) = world_gen_settings.get("dimensions") else {
        return Vec::new();
    };

    dimensions
        .keys()
        .filter(|dim| {
            !matches!(
                dim.as_bytes(),
                b"minecraft:overworld"
                    | b"minecraft:the_nether"
                    | b"minecraft:the_end"
                    | b"overworld"
                    | b"the_nether"
                    | b"the_end"
            )
        })
        .map(|dim| match dim.find(':') {
            Some(colon_index) => (&dim[..], &dim[..colon_index], &dim[colon_index + 1..]),
            None => (&dim[..], JavaStr::from_str("minecraft"), &dim[..]),
        })
        .collect()
}

fn get_generator<'a>(
    level_dat: &'a JCompound,
    dim_id: &(impl AsRef<JavaStr> + ?Sized),
) -> &'a JavaStr {
    let Some(JValue::Compound(world_gen_settings)) = level_dat.get("WorldGenSettings") else {
        return JavaStr::from_str("minecraft:noise");
    };
    let Some(JValue::Compound(dimensions)) = world_gen_settings.get("dimensions") else {
        return JavaStr::from_str("minecraft:noise");
    };
    let Some(JValue::Compound(dimension)) = dimensions.get(dim_id.as_ref()) else {
        return JavaStr::from_str("minecraft:noise");
    };
    let Some(JValue::Compound(generator)) = dimension.get("generator") else {
        return JavaStr::from_str("minecraft:noise");
    };
    let Some(JValue::String(gen_type)) = generator.get("type") else {
        return JavaStr::from_str("minecraft:noise");
    };
    &gen_type[..]
}

pub fn upgrade_dimensions(world: &Path, to_version: u32, dry_run: bool, level_dat: &JCompound) {
    let _span = info_span!("Upgrading dimensions").entered();

    let span = info_span!("Upgrading dimension", message = "the overworld").entered();
    upgrade_dimension(
        JavaStr::from_str("minecraft:overworld"),
        get_generator(level_dat, "minecraft:overworld"),
        world,
        world,
        to_version,
        dry_run,
    );
    span.exit();

    let span = info_span!("Upgrading dimension", message = "the nether").entered();
    upgrade_dimension(
        JavaStr::from_str("minecraft:the_nether"),
        get_generator(level_dat, "minecraft:the_nether"),
        world,
        &world.join("DIM-1"),
        to_version,
        dry_run,
    );
    span.exit();

    let span = info_span!("Upgrading dimension", message = "the end").entered();
    upgrade_dimension(
        JavaStr::from_str("minecraft:the_end"),
        get_generator(level_dat, "minecraft:the_end"),
        world,
        &world.join("DIM1"),
        to_version,
        dry_run,
    );
    span.exit();

    for (dim_id, dim_namespace, dim_path) in get_custom_dimensions(level_dat) {
        let _span = info_span!(
            "Upgrading dimension",
            message = dim_id.as_str_lossy().as_ref()
        )
        .entered();
        let mut dimension_dir = world.join(dim_namespace.as_str_lossy().as_ref());
        for part in dim_path.split('/') {
            dimension_dir.push(part.as_str_lossy().as_ref());
        }
        upgrade_dimension(
            dim_id,
            get_generator(level_dat, dim_id),
            world,
            &dimension_dir,
            to_version,
            dry_run,
        );
    }

    if !dry_run {
        delete_legacy_dat_files(world);
    }
}

fn upgrade_raids(dim_id: &JavaStr, dim_folder: &Path, to_version: u32, dry_run: bool) {
    if to_version < FIRST_RAIDS_VERSION {
        return;
    }

    if to_version >= NETHER_RAIDS_RENAME && dim_id == "minecraft:the_nether" {
        // move raids_nether.dat to raids.dat
        // note that vanilla doesn't do this and the old raids get lost
        let raids_file = dim_folder.join("data").join("raids.dat");
        if !raids_file.exists() {
            let raids_nether_file = dim_folder.join("data").join("raids_nether.dat");
            if dry_run {
                upgrade_data(
                    dim_folder,
                    "raids_nether",
                    types::saved_data_raids,
                    to_version,
                    dry_run,
                );
            } else if let Err(err) = std::fs::rename(raids_nether_file, raids_file) {
                if err.kind() != ErrorKind::NotFound {
                    error!("Error renaming raids_nether.dat to raids.dat: {err}");
                }
                return;
            }
        }
    }

    let raids_file = if dim_id == "minecraft:the_end" {
        "raids_end"
    } else if to_version < NETHER_RAIDS_RENAME && dim_id == "minecraft:the_nether" {
        "raids_nether"
    } else {
        "raids"
    };
    upgrade_data(
        dim_folder,
        raids_file,
        types::saved_data_raids,
        to_version,
        dry_run,
    );
}

fn upgrade_dimension(
    dim_id: &JavaStr,
    generator_type: &JavaStr,
    world_folder: &Path,
    dimension: &Path,
    to_version: u32,
    dry_run: bool,
) {
    // Upgrade entity chunks before regions, as regions may write to entities
    upgrade_entities(dimension, to_version, dry_run);

    upgrade_chunks(
        dim_id,
        generator_type,
        world_folder,
        dimension,
        to_version,
        dry_run,
    );

    upgrade_poi(dimension, to_version, dry_run);

    upgrade_raids(dim_id, dimension, to_version, dry_run);
}
