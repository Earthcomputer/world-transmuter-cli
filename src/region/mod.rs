mod chunk;

use crate::upgrade;
use java_string::JavaString;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{error, info, info_span, Span};
use valence_anvil::{RawChunk, RegionError, RegionFolder};
use world_transmuter::types;
use world_transmuter_engine::JCompound;

pub use chunk::{delete_legacy_dat_files, upgrade_chunks};

const SEPARATE_ENTITIES_VERSION: u32 = 2681; // 20w45a
const FIRST_POI_VERSION: u32 = 1937; // 19w11a

pub fn upgrade_entities(dimension: &Path, to_version: u32, dry_run: bool) {
    if to_version < SEPARATE_ENTITIES_VERSION {
        return;
    }

    let _span = info_span!("Upgrading entities").entered();
    upgrade_regions(
        &dimension.join("entities"),
        dry_run,
        |chunk_x, chunk_z, chunk, _| {
            upgrade(
                types::entity_chunk,
                chunk,
                || format!("chunk at {chunk_x}, {chunk_z}"),
                to_version,
                SEPARATE_ENTITIES_VERSION,
            )
        },
        || (),
    );
}

pub fn upgrade_poi(dimension: &Path, to_version: u32, dry_run: bool) {
    if to_version < FIRST_POI_VERSION {
        return;
    }

    let _span = info_span!("Upgrading poi").entered();

    let poi_path = dimension.join("poi");
    match poi_path.try_exists() {
        Ok(true) => {
            upgrade_regions(
                &poi_path,
                dry_run,
                |chunk_x, chunk_z, chunk, _| {
                    upgrade(
                        types::poi_chunk,
                        chunk,
                        || format!("chunk at {chunk_x}, {chunk_z}"),
                        to_version,
                        FIRST_POI_VERSION,
                    )
                },
                || (),
            )
        }
        Ok(false) => {}
        Err(err) => {
            error!("Error checking if poi exists, skipping: {err}");
        }
    };
}

fn upgrade_regions<S>(
    regions_path: &Path,
    dry_run: bool,
    do_update: impl Send + Sync + Fn(i32, i32, &mut JCompound, &mut S) -> bool,
    thread_local_state_init: impl Send + Sync + Fn() -> S,
) {
    // figure out which chunks exist
    info!("Counting chunks");
    let mut region_folder = RegionFolder::new(regions_path);
    let mut num_errors: usize = 0;
    let chunk_positions: Vec<_> = match region_folder.all_chunk_positions() {
        Ok(chunk_positions_iter) => chunk_positions_iter
            .filter_map(|pos| match pos {
                Ok(pos) => Some(pos),
                Err(err) => {
                    error!("Error listing chunks: {err}");
                    num_errors += 1;
                    None
                }
            })
            .collect(),
        Err(RegionError::Io(err)) if err.kind() == ErrorKind::NotFound => Vec::new(),
        Err(err) => {
            error!("Error listing chunks: {err}");
            return;
        }
    };
    drop(region_folder);
    if num_errors > 0 {
        error!("Found {num_errors} errors listing chunks");
    }

    let _span = info_span!(
        "Upgrading chunks",
        message = format!("count = {}", chunk_positions.len())
    )
    .entered();

    // partition the chunks into regions to make sure that region files are not overwritten concurrently
    let mut partitioned_chunks = HashMap::<(i32, i32), Vec<(i32, i32)>>::new();
    for chunk_pos @ (chunk_x, chunk_z) in chunk_positions {
        partitioned_chunks
            .entry((chunk_x >> 5, chunk_z >> 5))
            .or_default()
            .push(chunk_pos);
    }

    // upgrade the chunks
    let num_errors = AtomicUsize::new(0);
    let parent_span = Span::current();
    partitioned_chunks
        .into_values()
        .collect::<Vec<_>>()
        .into_par_iter()
        .for_each_init(
            move || {
                (
                    RegionFolder::new(regions_path),
                    thread_local_state_init(),
                    parent_span.clone().entered(),
                )
            },
            |(region_folder, thread_local_state, _), chunks| {
                for (chunk_x, chunk_z) in chunks {
                    let mut chunk_nbt: RawChunk<JavaString> =
                        match region_folder.get_chunk(chunk_x, chunk_z) {
                            Ok(Some(chunk_nbt)) => chunk_nbt,
                            Ok(None) => {
                                // all chunk positions listed the chunk, but it wasn't found when we tried to get it
                                num_errors.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            Err(err) => {
                                error!("Error reading chunk at {chunk_x}, {chunk_z}: {err}");
                                num_errors.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        };

                    if do_update(chunk_x, chunk_z, &mut chunk_nbt.data, thread_local_state)
                        && !dry_run
                    {
                        if let Err(err) = region_folder.set_chunk(chunk_x, chunk_z, &chunk_nbt.data)
                        {
                            error!("Error writing chunk at {chunk_x}, {chunk_z}: {err}");
                        }
                    }
                }
            },
        );

    let num_errors = num_errors.load(Ordering::Acquire);
    if num_errors > 0 {
        error!("Encountered {num_errors} errors while upgrading chunks");
    }
}
