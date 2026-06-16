//! Parse and classify raw telemetry lines.
//!
//! Two-stage pipeline:
//!   1. `parse_line` — split `key=value, key=value` into a map
//!   2. `classify`   — match the exact key set and convert to typed fields
//!
//! Records that don't match any known key set, or have invalid values, are
//! rejected with a descriptive error so the classifier can log and skip them.

use std::collections::HashMap;

use crate::error::{AppError, Result};
use crate::model::{
    DoorRecord, GpsRecord, PassengerRecord, RecordType, TelemetryRecord, VehicleRecord,
};

/// Parse a newline-terminated `key=value, key=value` line into key/value pairs.
///
/// Splits on `", "` (comma-space) as emitted by the simulator. A missing
/// separator produces merged segments like `longitude=2.0speed=3.0` which
/// fail here or at classification time.
pub fn parse_line(line: &str) -> Result<HashMap<String, String>> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(AppError::Parse("empty line".into()));
    }

    let mut map = HashMap::new();

    // Each segment should be exactly "key=value".
    for segment in trimmed.split(", ") {
        let segment = segment.trim();
        if segment.is_empty() {
            return Err(AppError::Parse("empty segment between separators".into()));
        }

        // Split on the first '=' only — values may contain '=' in theory,
        // though the simulator never emits them.
        let Some((key, value)) = segment.split_once('=') else {
            return Err(AppError::Parse(format!(
                "segment missing '=' (possible concatenation error): {segment}"
            )));
        };

        if key.is_empty() {
            return Err(AppError::Parse("empty key".into()));
        }
        if value.is_empty() {
            return Err(AppError::Parse(format!("empty value for key '{key}'")));
        }

        // Reject duplicate keys — shouldn't happen on clean data.
        if map.insert(key.to_owned(), value.to_owned()).is_some() {
            return Err(AppError::Parse(format!("duplicate key '{key}'")));
        }
    }

    if map.is_empty() {
        return Err(AppError::Parse("no key=value pairs found".into()));
    }

    Ok(map)
}

/// Classify and convert parsed fields into a typed telemetry record.
///
/// Classification is strict: the key set must match **exactly** — no extra
/// keys, no missing keys, no typos. This keeps bad data out of the JSONL files.
pub fn classify(fields: HashMap<String, String>) -> Result<TelemetryRecord> {
    let keys: Vec<&str> = fields.keys().map(String::as_str).collect();
    let key_set: std::collections::HashSet<&str> = keys.iter().copied().collect();

    // --- GPS: latitude, longitude, speed, altitude ---
    if key_set
        == ["latitude", "longitude", "speed", "altitude"]
            .into_iter()
            .collect()
    {
        return Ok(TelemetryRecord::Gps(GpsRecord {
            latitude: parse_f64(fields.get("latitude"), "latitude")?,
            longitude: parse_f64(fields.get("longitude"), "longitude")?,
            speed: parse_f64(fields.get("speed"), "speed")?,
            altitude: parse_f64(fields.get("altitude"), "altitude")?,
        }));
    }

    // --- Door: single "door" field, value must be "open" or "close" ---
    if key_set == ["door"].into_iter().collect() {
        let door = fields
            .get("door")
            .ok_or_else(|| AppError::Parse("missing door".into()))?
            .clone();
        if door != "open" && door != "close" {
            return Err(AppError::Parse(format!(
                "invalid door value '{door}' (expected open or close)"
            )));
        }
        return Ok(TelemetryRecord::Door(DoorRecord { door }));
    }

    // --- Vehicle: odometer, fuel_consumption, turn_direction ---
    if key_set
        == ["odometer", "fuel_consumption", "turn_direction"]
            .into_iter()
            .collect()
    {
        let turn_direction = fields
            .get("turn_direction")
            .ok_or_else(|| AppError::Parse("missing turn_direction".into()))?
            .clone();
        if !matches!(turn_direction.as_str(), "left" | "right" | "straight") {
            return Err(AppError::Parse(format!(
                "invalid turn_direction '{turn_direction}'"
            )));
        }
        return Ok(TelemetryRecord::Vehicle(VehicleRecord {
            odometer: parse_f64(fields.get("odometer"), "odometer")?,
            fuel_consumption: parse_f64(fields.get("fuel_consumption"), "fuel_consumption")?,
            turn_direction,
        }));
    }

    // --- Passenger: passengers_in, passengers_out, total_load ---
    if key_set
        == ["passengers_in", "passengers_out", "total_load"]
            .into_iter()
            .collect()
    {
        return Ok(TelemetryRecord::Passenger(PassengerRecord {
            passengers_in: parse_u32(fields.get("passengers_in"), "passengers_in")?,
            passengers_out: parse_u32(fields.get("passengers_out"), "passengers_out")?,
            total_load: parse_u32(fields.get("total_load"), "total_load")?,
        }));
    }

    // Key set didn't match any known type — sort for stable log output.
    let mut sorted = keys;
    sorted.sort_unstable();
    Err(AppError::UnknownRecord(sorted.join(", ")))
}

/// Full parse + classify pipeline for a single datagram line.
pub fn process_line(line: &str) -> Result<(TelemetryRecord, RecordType)> {
    let fields = parse_line(line)?;
    let record = classify(fields)?;
    let record_type = record.record_type();
    Ok((record, record_type))
}

/// Parse a string field as `f64`. Rejects non-numeric values (e.g. typos).
fn parse_f64(value: Option<&String>, field: &str) -> Result<f64> {
    let raw = value.ok_or_else(|| AppError::Parse(format!("missing {field}")))?;
    raw.parse::<f64>()
        .map_err(|_| AppError::Parse(format!("invalid numeric value for '{field}': {raw}")))
}

/// Parse a string field as `u32`. Used for passenger counts.
fn parse_u32(value: Option<&String>, field: &str) -> Result<u32> {
    let raw = value.ok_or_else(|| AppError::Parse(format!("missing {field}")))?;
    raw.parse::<u32>()
        .map_err(|_| AppError::Parse(format!("invalid integer value for '{field}': {raw}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_gps_line() {
        let fields =
            parse_line("latitude=51.6, longitude=-0.3, speed=42.7, altitude=38.2\n").unwrap();
        let record = classify(fields).unwrap();
        match record {
            TelemetryRecord::Gps(gps) => {
                assert!((gps.latitude - 51.6).abs() < f64::EPSILON);
                assert!((gps.longitude - (-0.3)).abs() < f64::EPSILON);
            }
            _ => panic!("expected gps"),
        }
    }

    #[test]
    fn rejects_concatenation_error() {
        // Missing ", " between longitude and speed merges pairs — won't classify as GPS.
        let err = process_line("latitude=1.0, longitude=2.0speed=3.0, altitude=4.0").unwrap_err();
        assert!(matches!(
            err,
            AppError::UnknownRecord(_) | AppError::Parse(_)
        ));
    }

    #[test]
    fn rejects_unknown_keys() {
        // Typo in "latitude" → key set won't match GPS.
        let fields = parse_line("latitide=1.0, longitude=2.0, speed=3.0, altitude=4.0").unwrap();
        let err = classify(fields).unwrap_err();
        assert!(matches!(err, AppError::UnknownRecord(_)));
    }
}
