"""Read-only central lock status via the Polestar C3 gRPC ExteriorService.

Deliberately minimal and read-only: fetches exactly one field (central lock
status) via a single unary-unary RPC on this integration's own already-
authenticated ``grpc_client.c3_channel`` / ``auth.access_token`` — no new
credentials, no lock/unlock/window/climate commands.

The wire-format encode/decode here needs no compiled .proto (there is no
public Polestar/Volvo .proto for ExteriorService) — protobuf's wire format
is self-describing enough that a ~15-line varint/length-delimited decoder
covers it. Ported and trimmed from kildahldev/unofficial-polestar-api
(MIT licensed), which reverse-engineered the field layout by observing the
official Android app.
"""

from __future__ import annotations

import logging
import uuid
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import grpc

_LOGGER = logging.getLogger(__name__)

EXTERIOR_SERVICE_METHOD = "/services.vehiclestates.exterior.ExteriorService/GetLatestExterior"


class CentralLockStatus(IntEnum):
    """Mirrors kildahldev's LockStatus enum for the ExteriorService."""

    UNSPECIFIED = 0
    UNLOCKED = 1
    LOCKED = 2


def _encode_varint(value: int) -> bytes:
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)


def _encode_string(field_number: int, value: str) -> bytes:
    encoded = value.encode("utf-8")
    tag = _encode_varint((field_number << 3) | 2)
    return tag + _encode_varint(len(encoded)) + encoded


def _encode_vehicle_request(vin: str) -> bytes:
    """VehicleRequest{id: string, vin: string} — fields 1, 2 (same envelope pypolestar's own GetBatteryRequest uses)."""
    return _encode_string(1, str(uuid.uuid4())) + _encode_string(2, vin)


def _decode_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        b = data[pos]
        result |= (b & 0x7F) << shift
        pos += 1
        if not (b & 0x80):
            break
        shift += 7
    return result, pos


def _decode_top_level_fields(data: bytes) -> dict[int, object]:
    """Minimal protobuf decoder — only enough to find the fields we need."""
    result: dict[int, object] = {}
    pos = 0
    while pos < len(data):
        tag, pos = _decode_varint(data, pos)
        field_number, wire_type = tag >> 3, tag & 0x07
        if wire_type == 0:
            value, pos = _decode_varint(data, pos)
        elif wire_type == 1:
            value, pos = data[pos : pos + 8], pos + 8
        elif wire_type == 2:
            length, pos = _decode_varint(data, pos)
            value, pos = data[pos : pos + length], pos + length
        elif wire_type == 5:
            value, pos = data[pos : pos + 4], pos + 4
        else:
            _LOGGER.debug("Unsupported exterior wire type %s, stopping decode", wire_type)
            break
        result[field_number] = value
    return result


def _parse_central_lock_status(response_bytes: bytes) -> CentralLockStatus | None:
    """Parse a GetLatestExteriorResponse and return the central lock status.

    Only handles the "Digital Twin" flat-field wire format (field 2 of the
    embedded ``exterior`` message is the central lock status enum directly,
    field 3 of the top-level response holds the embedded exterior message)
    — this is the current format as of 2026. The legacy nested-message
    format isn't implemented: this is a read-only informational sensor, and
    surfacing "unknown" beats guessing wrong.
    """
    top = _decode_top_level_fields(response_bytes)
    exterior_bytes = top.get(3)
    if not isinstance(exterior_bytes, bytes):
        return None
    exterior = _decode_top_level_fields(exterior_bytes)
    lock_value = exterior.get(2)
    if not isinstance(lock_value, int):
        return None
    try:
        return CentralLockStatus(lock_value)
    except ValueError:
        _LOGGER.debug("Unrecognized central lock status value %s", lock_value)
        return None


async def async_get_central_lock_status(
    channel: grpc.aio.Channel, vin: str, access_token: str
) -> CentralLockStatus | None:
    """Fetch the car's central lock status (read-only) over an existing C3 gRPC channel.

    Best-effort by design, mirroring how PolestarApi._update_grpc_data treats
    the existing get_battery/get_target_soc gRPC calls as non-fatal — a
    failure here should never take down battery/odometer/health data.
    """
    try:
        call = channel.unary_unary(
            EXTERIOR_SERVICE_METHOD,
            request_serializer=lambda data: data,
            response_deserializer=lambda data: data,
        )
        response_bytes = await call(
            _encode_vehicle_request(vin),
            metadata=[("authorization", f"Bearer {access_token}"), ("vin", vin)],
            timeout=30,
        )
    except Exception as exc:  # noqa: BLE001 - best-effort, same pattern as get_battery callers
        _LOGGER.debug("GetLatestExterior failed for VIN %s: %s", vin, exc)
        return None
    return _parse_central_lock_status(response_bytes)
