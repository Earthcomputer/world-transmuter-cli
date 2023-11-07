use crate::data::read_data;
use crate::region::{upgrade_regions, SEPARATE_ENTITIES_VERSION};
use crate::upgrade;
use ahash::{AHashMap, AHashSet};
use java_string::{JavaStr, JavaString};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::OnceLock;
use tracing::{error, info_span};
use valence_anvil::RegionFolder;
use valence_nbt::{compound, jcompound};
use world_transmuter::{static_string_map, static_string_set, types};
use world_transmuter_engine::{JCompound, JList, JValue};

const LAST_MONOLITH_STRUCTURE_DATA_VERSION: u32 = 1493; // 18w20c

static_string_map! {
    CURRENT_TO_LEGACY_MAP, current_to_legacy_map, {
        "Village" => "Village",
        "Mineshaft" => "Mineshaft",
        "Mansion" => "Mansion",
        "Igloo" => "Temple",
        "Desert_Pyramid" => "Temple",
        "Jungle_Pyramid" => "Temple",
        "Swamp_Hut" => "Temple",
        "Stronghold" => "Stronghold",
        "Monument" => "Monument",
        "Fortress" => "Fortress",
        "EndCity" => "EndCity",
    }
}

static_string_map! {
    LEGACY_TO_CURRENT_MAP, legacy_to_current_map, {
        "Iglu" => "Igloo",
        "TeDP" => "Desert_Pyramid",
        "TeJP" => "Jungle_Pyramid",
        "TeSH" => "Swamp_Hut",
    }
}

static_string_set! {
    OLD_STRUCTURE_REGISTRY_KEYS, old_structure_registry_keys, {
        "pillager_outpost",
        "mineshaft",
        "mansion",
        "jungle_pyramid",
        "desert_pyramid",
        "igloo",
        "ruined_portal",
        "shipwreck",
        "swamp_hut",
        "stronghold",
        "monument",
        "ocean_ruin",
        "fortress",
        "endcity",
        "buried_treasure",
        "village",
        "nether_fossil",
        "bastion_remnant",
    }
}

const OVERWORLD_LEGACY_KEYS: [&JavaStr; 6] = [
    JavaStr::from_str("Monument"),
    JavaStr::from_str("Stronghold"),
    JavaStr::from_str("Village"),
    JavaStr::from_str("Mineshaft"),
    JavaStr::from_str("Temple"),
    JavaStr::from_str("Mansion"),
];

const OVERWORLD_CURRENT_KEYS: [&JavaStr; 8] = [
    JavaStr::from_str("Village"),
    JavaStr::from_str("Mineshaft"),
    JavaStr::from_str("Mansion"),
    JavaStr::from_str("Igloo"),
    JavaStr::from_str("Desert_Pyramid"),
    JavaStr::from_str("Jungle_Pyramid"),
    JavaStr::from_str("Swamp_Hut"),
    JavaStr::from_str("Monument"),
];

const NETHER_KEYS: [&JavaStr; 1] = [JavaStr::from_str("Fortress")];

const END_KEYS: [&JavaStr; 1] = [JavaStr::from_str("EndCity")];

struct LegacyStructureDataHandler {
    has_legacy_data: bool,
    data_map: BTreeMap<JavaString, AHashMap<(i32, i32), JCompound>>,
    index_map: BTreeMap<&'static JavaStr, StructureFeatureIndexSavedData>,
    legacy_keys: &'static [&'static JavaStr],
    current_keys: &'static [&'static JavaStr],
}

impl LegacyStructureDataHandler {
    fn new(
        world_folder: &Path,
        legacy_keys: &'static [&'static JavaStr],
        current_keys: &'static [&'static JavaStr],
    ) -> Self {
        let mut result = Self {
            legacy_keys,
            current_keys,
            has_legacy_data: false,
            data_map: BTreeMap::new(),
            index_map: BTreeMap::new(),
        };

        result.populate_caches(world_folder);
        result.has_legacy_data = current_keys
            .iter()
            .any(|key| result.data_map.contains_key(*key));
        result
    }

    fn populate_caches(&mut self, world_folder: &Path) {
        for legacy_key in self.legacy_keys {
            let mut data = match read_data(world_folder, legacy_key.as_str_lossy()) {
                Ok(Some(data)) => data,
                Ok(None) => {
                    error!("Failed to parse {legacy_key}.dat");
                    continue;
                }
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    error!("Failed to read {legacy_key}.dat: {err}");
                    continue;
                }
            };
            if !upgrade(
                types::saved_data_structure_feature_indices,
                &mut data,
                || format!("{legacy_key}.dat"),
                LAST_MONOLITH_STRUCTURE_DATA_VERSION,
                99,
            ) {
                continue;
            }
            let Some(JValue::Compound(mut data)) = data.remove("data") else {
                continue;
            };
            let Some(JValue::Compound(features)) = data.remove("Features") else {
                continue;
            };
            if features.is_empty() {
                continue;
            }

            let index_key = (*legacy_key).to_owned() + "_index";
            let Some(index_saved_data) =
                StructureFeatureIndexSavedData::load(world_folder, index_key)
            else {
                continue;
            };

            let mut chunks = Vec::new();
            for (_, feature) in features {
                let JValue::Compound(mut feature) = feature else {
                    continue;
                };
                let chunk_x = feature.get("ChunkX").and_then(|v| v.as_i32()).unwrap_or(0);
                let chunk_z = feature.get("ChunkZ").and_then(|v| v.as_i32()).unwrap_or(0);
                chunks.push((chunk_x, chunk_z));
                if let Some(JValue::List(JList::Compound(children))) = feature.get("Children") {
                    if let Some(JValue::String(id)) =
                        children.first().and_then(|child| child.get("id"))
                    {
                        if let Some(current_id) = legacy_to_current_map().get(&id[..]) {
                            feature.insert("id", *current_id);
                        }
                    }
                }

                if let Some(JValue::String(id)) = feature.get("id") {
                    self.data_map
                        .entry(id.clone())
                        .or_default()
                        .insert((chunk_x, chunk_z), feature);
                }
            }

            if !index_saved_data.all.is_empty() {
                self.index_map.insert(*legacy_key, index_saved_data);
            } else {
                let mut index_saved_data = StructureFeatureIndexSavedData::new();
                for (chunk_x, chunk_z) in chunks {
                    index_saved_data.add_index(chunk_x, chunk_z);
                }
                self.index_map.insert(*legacy_key, index_saved_data);
            }
        }
    }

    fn get(dimension: &JavaStr, world_folder: &Path) -> Option<Self> {
        if dimension == "minecraft:overworld" {
            Some(Self::new(
                world_folder,
                &OVERWORLD_LEGACY_KEYS,
                &OVERWORLD_CURRENT_KEYS,
            ))
        } else if dimension == "minecraft:the_nether" {
            Some(Self::new(world_folder, &NETHER_KEYS, &NETHER_KEYS))
        } else if dimension == "minecraft:the_end" {
            Some(Self::new(world_folder, &END_KEYS, &END_KEYS))
        } else {
            error!("Custom dimension {dimension} had too old chunk version");
            None
        }
    }

    fn update_from_legacy(&self, chunk: &mut JCompound) {
        let Some(JValue::Compound(level)) = chunk.get_mut("Level") else {
            return;
        };
        let chunk_x = level.get("xPos").and_then(|v| v.as_i32()).unwrap_or(0);
        let chunk_z = level.get("zPos").and_then(|v| v.as_i32()).unwrap_or(0);

        if !matches!(level.get("Structures"), Some(JValue::Compound(_))) {
            level.insert("Structures", JCompound::new());
        }
        let Some(JValue::Compound(structures)) = level.get_mut("Structures") else {
            unreachable!();
        };

        if self.is_unhandled_structure_start(chunk_x, chunk_z) {
            self.update_structure_start(structures, chunk_x, chunk_z);
        }

        if !matches!(structures.get("References"), Some(JValue::Compound(_))) {
            structures.insert("References", JCompound::new());
        }
        let Some(JValue::Compound(references)) = structures.get_mut("References") else {
            unreachable!();
        };

        for current_key in self.current_keys {
            let is_old_structure =
                old_structure_registry_keys().contains(&current_key.to_ascii_lowercase()[..]);
            if !matches!(references.get(*current_key), Some(JValue::LongArray(_)))
                && is_old_structure
            {
                let mut starts = Vec::new();
                for x in chunk_x - 8..=chunk_x + 8 {
                    for z in chunk_z - 8..=chunk_z + 8 {
                        if self.has_legacy_start(x, z, current_key) {
                            starts.push(((x as u32 as u64) | (z as u32 as u64) << 32) as i64);
                        }
                    }
                }
                references.insert(*current_key, starts);
            }
        }
    }

    fn is_unhandled_structure_start(&self, chunk_x: i32, chunk_z: i32) -> bool {
        if !self.has_legacy_data {
            return false;
        }

        self.current_keys.iter().any(|current_key| {
            self.data_map.contains_key(*current_key)
                && self
                    .index_map
                    .get(current_to_legacy_map().get(*current_key).unwrap())
                    .unwrap()
                    .has_unhandled_index(chunk_x, chunk_z)
        })
    }

    fn update_structure_start(&self, structures: &mut JCompound, chunk_x: i32, chunk_z: i32) {
        if !matches!(structures.get("Starts"), Some(JValue::Compound(_))) {
            structures.insert("Starts", JCompound::new());
        }
        let Some(JValue::Compound(starts)) = structures.get_mut("Starts") else {
            unreachable!()
        };

        for current_key in self.current_keys {
            if let Some(data_map) = self.data_map.get(*current_key) {
                if self
                    .index_map
                    .get(current_to_legacy_map().get(*current_key).unwrap())
                    .unwrap()
                    .has_unhandled_index(chunk_x, chunk_z)
                {
                    if let Some(data) = data_map.get(&(chunk_x, chunk_z)) {
                        starts.insert(*current_key, data.clone());
                    }
                }
            }
        }
    }

    fn has_legacy_start(&self, chunk_x: i32, chunk_z: i32, typ: &JavaStr) -> bool {
        if !self.has_legacy_data {
            return false;
        }

        self.data_map.contains_key(typ)
            && self
                .index_map
                .get(current_to_legacy_map().get(typ).unwrap())
                .unwrap()
                .has_start_index(chunk_x, chunk_z)
    }
}

struct StructureFeatureIndexSavedData {
    all: AHashSet<(i32, i32)>,
    remaining: AHashSet<(i32, i32)>,
}

impl StructureFeatureIndexSavedData {
    fn new() -> Self {
        Self {
            all: AHashSet::new(),
            remaining: AHashSet::new(),
        }
    }

    fn load(world_folder: &Path, index_key: JavaString) -> Option<Self> {
        let mut data = match read_data(world_folder, index_key.as_str_lossy()) {
            Ok(Some(data)) => data,
            Ok(None) => {
                error!("Failed to parse {index_key}.dat");
                return None;
            }
            Err(err) if err.kind() == ErrorKind::NotFound => JCompound::new(),
            Err(err) => {
                error!("Failed to read {index_key}.dat: {err}");
                return None;
            }
        };
        if !upgrade(
            types::saved_data_structure_feature_indices,
            &mut data,
            || format!("{index_key}.dat"),
            LAST_MONOLITH_STRUCTURE_DATA_VERSION,
            99,
        ) {
            return None;
        }

        let mut result = Self::new();

        if let Some(JValue::LongArray(all)) = data.get("All") {
            for all in all {
                let all = *all as u64;
                result
                    .all
                    .insert((all as u32 as i32, (all >> 32) as u32 as i32));
            }
        }
        if let Some(JValue::LongArray(remaining)) = data.get("Remaining") {
            for remaining in remaining {
                let remaining = *remaining as u64;
                result
                    .remaining
                    .insert((remaining as u32 as i32, (remaining >> 32) as u32 as i32));
            }
        }

        Some(result)
    }

    fn add_index(&mut self, chunk_x: i32, chunk_z: i32) {
        self.all.insert((chunk_x, chunk_z));
        self.remaining.insert((chunk_x, chunk_z));
    }

    fn has_start_index(&self, chunk_x: i32, chunk_z: i32) -> bool {
        self.all.contains(&(chunk_x, chunk_z))
    }

    fn has_unhandled_index(&self, chunk_x: i32, chunk_z: i32) -> bool {
        self.remaining.contains(&(chunk_x, chunk_z))
    }
}

fn update_chunk_from_legacy(
    dim_id: &JavaStr,
    world_folder: &Path,
    legacy_structure_handler: &OnceLock<Option<LegacyStructureDataHandler>>,
    chunk: &mut JCompound,
) {
    let Some(JValue::Compound(level)) = chunk.get_mut("Level") else {
        return;
    };
    if !level
        .get("hasLegacyStructureData")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return;
    }

    let legacy_structure_handler = legacy_structure_handler
        .get_or_init(|| LegacyStructureDataHandler::get(dim_id, world_folder))
        .as_ref();
    if let Some(legacy_structure_handler) = legacy_structure_handler {
        legacy_structure_handler.update_from_legacy(chunk);
    }
}

pub fn upgrade_chunks(
    dim_id: &JavaStr,
    generator_type: &JavaStr,
    world_folder: &Path,
    dimension: &Path,
    to_version: u32,
    dry_run: bool,
) {
    let _span = info_span!("Upgrading regions").entered();

    if !dry_run && to_version >= SEPARATE_ENTITIES_VERSION {
        if let Err(err) = std::fs::create_dir(dimension.join("entities")) {
            if err.kind() != ErrorKind::AlreadyExists {
                error!("Failed to create entity region dir: {err}");
            }
        }
    }

    let legacy_structure_handler = OnceLock::new();

    upgrade_regions::<RegionFolder>(
        &dimension.join("region"),
        dry_run,
        |chunk_x, chunk_z, chunk, entity_region_folder| {
            let version = chunk
                .get("DataVersion")
                .and_then(|v| v.as_i32())
                .map(|v| v as u32)
                .unwrap_or(99);
            if version < LAST_MONOLITH_STRUCTURE_DATA_VERSION {
                if !upgrade(
                    types::chunk,
                    chunk,
                    || format!("chunk at {chunk_x}, {chunk_z}"),
                    LAST_MONOLITH_STRUCTURE_DATA_VERSION.min(to_version),
                    99,
                ) {
                    return false;
                }
                if to_version < LAST_MONOLITH_STRUCTURE_DATA_VERSION {
                    return true;
                }
                update_chunk_from_legacy(dim_id, world_folder, &legacy_structure_handler, chunk);
            }
            chunk.insert(
                "__context",
                jcompound! {
                    "dimension" => dim_id,
                    "generator" => generator_type,
                },
            );
            if !upgrade(
                types::chunk,
                chunk,
                || format!("chunk at {chunk_x}, {chunk_z}"),
                to_version,
                99,
            ) {
                return false;
            }
            chunk.remove("__context");

            if !dry_run
                && version < SEPARATE_ENTITIES_VERSION
                && to_version >= SEPARATE_ENTITIES_VERSION
            {
                // extract entities into separate region folder
                if let Some(JValue::Compound(level)) = chunk.get_mut("Level") {
                    if let Some(JValue::String(status)) = level.get("Status") {
                        if status == "full" || status == "minecraft:full" {
                            if let Some(entities) = level.remove("Entities") {
                                if let Err(err) = entity_region_folder.set_chunk(
                                    chunk_x,
                                    chunk_z,
                                    &jcompound! {
                                        "Entities" => entities,
                                    },
                                ) {
                                    error!(
                                        "Error writing entity chunk {chunk_x}, {chunk_z}: {err}"
                                    );
                                    return false;
                                }
                            }
                        }
                    }
                } else if let Some(JValue::String(status)) = chunk.get("Status") {
                    if status == "full" || status == "minecraft:full" {
                        if let Some(entities) = chunk.remove("entities") {
                            if let Err(err) = entity_region_folder.set_chunk(
                                chunk_x,
                                chunk_z,
                                &jcompound! {
                                    "Entities" => entities,
                                },
                            ) {
                                error!("Error writing entity chunk {chunk_x}, {chunk_z}: {err}");
                                return false;
                            }
                        }
                    }
                }
            }

            true
        },
        || RegionFolder::new(dimension.join("entities")),
    );
}

fn delete_legacy_dat_file(world_folder: &Path, key: &JavaStr) {
    if let Err(err) = std::fs::remove_file(world_folder.join("data").join(format!("{key}.dat"))) {
        if err.kind() != ErrorKind::NotFound {
            error!("Error deleting legacy {key}.dat file: {err}");
        }
    }
}

pub fn delete_legacy_dat_files(world_folder: &Path) {
    for key in OVERWORLD_LEGACY_KEYS {
        delete_legacy_dat_file(world_folder, key);
    }
    for key in NETHER_KEYS {
        delete_legacy_dat_file(world_folder, key);
    }
    for key in END_KEYS {
        delete_legacy_dat_file(world_folder, key);
    }
}
