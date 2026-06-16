//! Typed telemetry record definitions and output file mapping.
//!
//! Each variant maps 1:1 to a `.jsonl` output file. `serde` handles JSON
//! serialization; field names match the simulator's key names.

use serde::Serialize;

/// Identifies one of the four telemetry record categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordType {
    Gps,
    Door,
    Vehicle,
    Passenger,
}

impl RecordType {
    /// All record types — used when spawning one writer task per type.
    pub const ALL: [RecordType; 4] = [
        RecordType::Gps,
        RecordType::Door,
        RecordType::Vehicle,
        RecordType::Passenger,
    ];

    /// Output filename for this record type.
    pub fn filename(self) -> &'static str {
        match self {
            RecordType::Gps => "gps.jsonl",
            RecordType::Door => "door.jsonl",
            RecordType::Vehicle => "vehicle.jsonl",
            RecordType::Passenger => "passenger.jsonl",
        }
    }

    /// Short label used in log messages.
    pub fn label(self) -> &'static str {
        match self {
            RecordType::Gps => "gps",
            RecordType::Door => "door",
            RecordType::Vehicle => "vehicle",
            RecordType::Passenger => "passenger",
        }
    }
}

/// GPS position record: latitude, longitude, speed, altitude.
#[derive(Debug, Serialize)]
pub struct GpsRecord {
    pub latitude: f64,
    pub longitude: f64,
    pub speed: f64,
    pub altitude: f64,
}

/// Door state record: open or close.
#[derive(Debug, Serialize)]
pub struct DoorRecord {
    pub door: String,
}

/// Vehicle telemetry: odometer, fuel consumption, turn direction.
#[derive(Debug, Serialize)]
pub struct VehicleRecord {
    pub odometer: f64,
    pub fuel_consumption: f64,
    pub turn_direction: String,
}

/// Passenger counter: boardings, alightings, running total.
#[derive(Debug, Serialize)]
pub struct PassengerRecord {
    pub passengers_in: u32,
    pub passengers_out: u32,
    pub total_load: u32,
}

/// Union of all record types. `untagged` lets serde emit a flat JSON object
/// without a type discriminator field.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum TelemetryRecord {
    Gps(GpsRecord),
    Door(DoorRecord),
    Vehicle(VehicleRecord),
    Passenger(PassengerRecord),
}

impl TelemetryRecord {
    /// Map a parsed record to its output file category.
    pub fn record_type(&self) -> RecordType {
        match self {
            TelemetryRecord::Gps(_) => RecordType::Gps,
            TelemetryRecord::Door(_) => RecordType::Door,
            TelemetryRecord::Vehicle(_) => RecordType::Vehicle,
            TelemetryRecord::Passenger(_) => RecordType::Passenger,
        }
    }
}
