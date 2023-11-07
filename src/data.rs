use crate::upgrade;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::File;
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::RwLockReadGuard;
use tracing::{error, info_span};
use valence_nbt::{from_binary, to_binary};
use world_transmuter::types;
use world_transmuter_engine::{JCompound, MapDataType};

pub fn read_data(dim_folder: &Path, name: impl Into<String>) -> io::Result<Option<JCompound>> {
    let mut file = dim_folder.join("data");
    file.push(name.into() + ".dat");

    let mut file = File::open(file)?;

    let mut gzip_magic = [0; 2];
    let is_gzip = match file.read_exact(&mut gzip_magic) {
        Ok(()) => gzip_magic == [0x1f, 0x8b],
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => false,
        Err(err) => return Err(err),
    };

    file.seek(SeekFrom::Start(0))?;

    let mut contents = Vec::new();
    if is_gzip {
        GzDecoder::new(file).read_to_end(&mut contents)?;
    } else {
        file.read_to_end(&mut contents)?;
    }

    Ok(from_binary(&mut &*contents)
        .ok()
        .map(|(compound, _)| compound))
}

pub fn upgrade_data(
    dim_folder: &Path,
    name: impl Into<String>,
    typ: impl FnOnce() -> RwLockReadGuard<'static, MapDataType<'static>>,
    to_version: u32,
    dry_run: bool,
) {
    let name = name.into();

    let _span = info_span!("Upgrading data", message = name).entered();

    let mut data = match read_data(dim_folder, name.clone()) {
        Ok(Some(data)) => data,
        Ok(None) => {
            error!("Error reading {name}.dat");
            return;
        }
        Err(err) if err.kind() == ErrorKind::NotFound => return,
        Err(err) => {
            error!("Error reading {name}.dat: {err}");
            return;
        }
    };
    if !upgrade(typ, &mut data, || name.clone(), to_version, 99) {
        return;
    }

    if !dry_run {
        let file = match File::create(dim_folder.join("data").join(format!("{name}.dat"))) {
            Ok(file) => file,
            Err(err) => {
                error!("Error opening {name}.dat for write: {err}");
                return;
            }
        };
        if let Err(err) = to_binary(&data, GzEncoder::new(file, Compression::default()), "") {
            error!("Error writing to {name}.dat: {err}");
        }
    }
}

pub fn upgrade_map_data(world_folder: &Path, to_version: u32, dry_run: bool) {
    let _span = info_span!("Upgrading map data").entered();

    let idcounts = match read_data(world_folder, "idcounts") {
        Ok(Some(data)) => data,
        Ok(None) => {
            error!("Error reading idcounts.dat");
            return;
        }
        Err(err) if err.kind() == ErrorKind::NotFound => return,
        Err(err) => {
            error!("Error reading idcounts.dat: {err}");
            return;
        }
    };

    let Some(map_count) = idcounts.get("map").and_then(|v| v.as_i32()) else {
        return;
    };
    for map_id in 0..=map_count {
        upgrade_data(
            world_folder,
            format!("map_{map_id}"),
            types::saved_data_map_data,
            to_version,
            dry_run,
        );
    }
}
