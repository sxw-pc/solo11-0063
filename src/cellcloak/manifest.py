"""Immutable input-set identity and verified multi-table input capture.

A manifest binds one normalized-contract hash to an ordered list of table
sources: ``table_id``, contract-relative ``source`` path, raw SHA-256 and byte
size. ``run --manifest`` verifies that identity and builds every
:class:`~cellcloak.model.TableModel` from the verified bytes, never by
re-opening the live source path for parsing.

Two capture strategies are provided:

``bytes``
    Read every member into memory once, hash it, re-hash the live sources as a
    post-check, then parse the retained bytes. No temporary files are needed,
    but peak memory grows with the total size of all CSVs.

``snapshot`` (default)
    Copy every source into a private temporary directory while hashing the
    copied bytes, re-hash the live sources as a post-check, then parse only
    the snapshot files. Memory stays bounded, at the cost of extra I/O and
    temporary-directory handling.

Neither strategy can atomically capture a set of independent files on an
ordinary filesystem. A concurrent replacement that changes bytes is detected
by the before/after digest comparison. A replacement that is reverted before
either observation (ABA) cannot be proven absent; the manifest records what was
observed, not a filesystem transactional guarantee.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .contract import Contract, TableSpec
from .errors import ManifestError

MANIFEST_VERSION = 1
MANIFEST_KIND = "cellcloak-input-manifest"
MANIFEST_SHA_FIELD = "manifest_sha256"
INPUT_MODES = ("snapshot", "bytes")
DEFAULT_INPUT_MODE = "snapshot"
_TOP_LEVEL_KEYS = {"version", "kind", "dataset_name", "contract_hash", "members", MANIFEST_SHA_FIELD}
_MEMBER_KEYS = {"table_id", "source", "sha256", "size"}
_HEX_DIGITS = frozenset("0123456789abcdef")


def _err(code: str, message: str, **kwargs: Any) -> ManifestError:
    return ManifestError(code, message, **kwargs)


def _sha256_hex(value: Any, field: str, table_id: str | None = None) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in _HEX_DIGITS for char in value):
        raise _err(
            "E_MANIFEST_SCHEMA",
            "sha256 must be 64 lowercase hexadecimal characters",
            field=field,
            table_id=table_id,
        )
    return value


def _safe_relative_source(value: Any, table_id: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _err("E_MANIFEST_SCHEMA", "source must be a non-empty string", field="source", table_id=table_id)
    source = value.strip()
    parts = source.replace("\\", "/").split("/")
    if Path(source).is_absolute() or ".." in parts:
        raise _err(
            "E_MANIFEST_SOURCE_PATH",
            "source must be a relative path without an absolute root or parent traversal",
            field="source",
            table_id=table_id,
        )
    return source


def _size(value: Any, table_id: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _err("E_MANIFEST_SCHEMA", "size must be a non-negative integer", field="size", table_id=table_id)
    return value


@dataclass(frozen=True)
class ManifestMember:
    table_id: str
    source: str
    sha256: str
    size: int

    def as_record(self) -> dict[str, Any]:
        return {
            "table_id": self.table_id,
            "source": self.source,
            "sha256": self.sha256,
            "size": self.size,
        }


@dataclass(frozen=True)
class InputManifest:
    path: Path
    manifest_hash: str
    contract_hash: str
    dataset_name: str
    members: tuple[ManifestMember, ...]

    def member(self, table_id: str) -> ManifestMember:
        for member in self.members:
            if member.table_id == table_id:
                return member
        raise _err("E_MANIFEST_MEMBER_MISSING", "manifest has no member for table", table_id=table_id)

    def as_record(self) -> dict[str, Any]:
        return {
            "pinned": True,
            "mode": None,
            "manifest_hash": self.manifest_hash,
            "manifest_path": str(self.path),
            "contract_hash": self.contract_hash,
            "dataset_name": self.dataset_name,
            "members": [member.as_record() for member in self.members],
        }


def _canonical(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _manifest_payload(
    *,
    version: int,
    kind: str,
    dataset_name: str,
    contract_hash: str,
    members: tuple[ManifestMember, ...],
) -> dict[str, Any]:
    return {
        "version": version,
        "kind": kind,
        "dataset_name": dataset_name,
        "contract_hash": contract_hash,
        "members": [member.as_record() for member in members],
    }


def _digest_body(body: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(body)).hexdigest()


def _hash_stream(handle: Any) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while True:
        chunk = handle.read(1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _hash_file(path: Path) -> tuple[str, int]:
    with path.open("rb") as handle:
        return _hash_stream(handle)


def _resolve_source(contract: Contract, spec: TableSpec) -> Path:
    resolved = spec.source_path
    try:
        resolved = resolved.resolve()
        resolved.relative_to(contract.base_dir.resolve())
    except (OSError, ValueError) as exc:
        raise _err(
            "E_MANIFEST_SOURCE_PATH",
            "source escapes the contract directory",
            table_id=spec.table_id,
            path=spec.source,
        ) from exc
    return resolved


def _format_members(members: list[ManifestMember] | list[str]) -> str:
    labels: list[str] = []
    for member in members:
        if isinstance(member, str):
            labels.append(member)
        else:
            labels.append(f"{member.table_id}({member.source})")
    return ", ".join(labels) if labels else "(none)"


def create_manifest(contract: Contract, manifest_path: str | Path) -> InputManifest:
    """Hash the current ordered source set and write a self-checking manifest.

    Each source is hashed once, then hashed a second time after all members
    have been observed. Any difference aborts creation. This is a static-file
    operation: like the run-time capture, it cannot prove that an ABA rewrite
    did not occur between observations.
    """

    members: list[ManifestMember] = []
    for spec in contract.tables:
        path = _resolve_source(contract, spec)
        try:
            sha256, size = _hash_file(path)
        except OSError as exc:
            raise _err(
                "E_MANIFEST_SOURCE_READ",
                "cannot read manifest source",
                table_id=spec.table_id,
                path=spec.source,
            ) from exc
        members.append(ManifestMember(spec.table_id, spec.source, sha256, size))

    changed: list[ManifestMember] = []
    for member in members:
        spec = contract.table(member.table_id)
        path = _resolve_source(contract, spec)
        try:
            sha256, size = _hash_file(path)
        except OSError:
            changed.append(member)
            continue
        if sha256 != member.sha256 or size != member.size:
            changed.append(member)
    if changed:
        raise _err(
            "E_MANIFEST_SOURCE_CHANGED",
            f"source set changed while creating manifest: {_format_members(changed)}",
            field="members",
        )

    body = _manifest_payload(
        version=MANIFEST_VERSION,
        kind=MANIFEST_KIND,
        dataset_name=contract.dataset_name,
        contract_hash=contract.normalized_hash,
        members=tuple(members),
    )
    manifest_hash = _digest_body(body)
    document = dict(body)
    document[MANIFEST_SHA_FIELD] = manifest_hash

    target = Path(manifest_path).expanduser()
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise _err("E_MANIFEST_WRITE", "cannot create manifest directory", path=target) from exc
    temporary = target.with_name(target.name + ".tmp")
    try:
        temporary.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8", newline="\n")
        os.replace(temporary, target)
    except OSError as exc:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise _err("E_MANIFEST_WRITE", "cannot write manifest", path=target) from exc

    return InputManifest(
        path=target.resolve(),
        manifest_hash=manifest_hash,
        contract_hash=contract.normalized_hash,
        dataset_name=contract.dataset_name,
        members=tuple(members),
    )


def load_manifest(manifest_path: str | Path) -> InputManifest:
    path = Path(manifest_path).expanduser().resolve()
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise _err("E_MANIFEST_READ", "cannot read manifest", path=path) from exc
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise _err("E_MANIFEST_ENCODING", "manifest must be UTF-8 or UTF-8-SIG", path=path) from exc
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise _err("E_MANIFEST_YAML", "invalid manifest YAML", path=path) from exc
    if not isinstance(document, dict):
        raise _err("E_MANIFEST_SCHEMA", "manifest must be a mapping", path=path)

    unknown = sorted(set(document) - _TOP_LEVEL_KEYS)
    if unknown:
        raise _err("E_MANIFEST_SCHEMA", f"unknown manifest field(s): {', '.join(unknown)}", path=path)
    version = document.get("version")
    if isinstance(version, bool) or version != MANIFEST_VERSION:
        raise _err("E_MANIFEST_SCHEMA", "manifest version must be 1", field="version", path=path)
    if document.get("kind") != MANIFEST_KIND:
        raise _err("E_MANIFEST_SCHEMA", f"manifest kind must be {MANIFEST_KIND}", field="kind", path=path)
    dataset_name = document.get("dataset_name")
    if not isinstance(dataset_name, str) or not dataset_name.strip():
        raise _err("E_MANIFEST_SCHEMA", "dataset_name must be a non-empty string", field="dataset_name", path=path)
    contract_hash = _sha256_hex(document.get("contract_hash"), "contract_hash")

    raw_members = document.get("members")
    if not isinstance(raw_members, list) or not raw_members:
        raise _err("E_MANIFEST_SCHEMA", "members must be a non-empty list", field="members", path=path)

    members: list[ManifestMember] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_members):
        if not isinstance(item, dict):
            raise _err("E_MANIFEST_SCHEMA", "member must be a mapping", field=f"members[{index}]", path=path)
        unknown_member = sorted(set(item) - _MEMBER_KEYS)
        if unknown_member:
            raise _err(
                "E_MANIFEST_SCHEMA",
                f"unknown member field(s): {', '.join(unknown_member)}",
                field=f"members[{index}]",
                path=path,
            )
        table_id = item.get("table_id")
        if not isinstance(table_id, str) or not table_id.strip():
            raise _err("E_MANIFEST_SCHEMA", "table_id must be a non-empty string", field=f"members[{index}].table_id", path=path)
        table_id = table_id.strip()
        if table_id in seen:
            raise _err("E_MANIFEST_MEMBER_DUPLICATE", "manifest contains duplicate members", table_id=table_id, path=path)
        seen.add(table_id)
        source = _safe_relative_source(item.get("source"), table_id)
        sha256 = _sha256_hex(item.get("sha256"), f"members[{index}].sha256", table_id)
        size = _size(item.get("size"), table_id)
        members.append(ManifestMember(table_id, source, sha256, size))

    declared = document.get(MANIFEST_SHA_FIELD)
    signature = _sha256_hex(declared, MANIFEST_SHA_FIELD, None)
    body = {key: document[key] for key in _TOP_LEVEL_KEYS - {MANIFEST_SHA_FIELD}}
    if signature != _digest_body(body):
        raise _err(
            "E_MANIFEST_INTEGRITY",
            "manifest_sha256 does not match the manifest contents",
            field=MANIFEST_SHA_FIELD,
            path=path,
        )

    return InputManifest(
        path=path,
        manifest_hash=signature,
        contract_hash=contract_hash,
        dataset_name=dataset_name.strip(),
        members=tuple(members),
    )


def validate_manifest(contract: Contract, manifest: InputManifest) -> None:
    """Validate a manifest against the loaded contract and its table order.

    Member-level checks run before the normalized-contract hash check so a
    changed source path or table order is reported with the concrete member
    rather than only a contract-hash difference.
    """

    if manifest.dataset_name != contract.dataset_name:
        raise _err(
            "E_MANIFEST_CONTRACT_MISMATCH",
            "manifest dataset_name does not match the contract",
            field="dataset_name",
            path=manifest.path,
        )

    expected_ids = [spec.table_id for spec in contract.tables]
    actual_ids = [member.table_id for member in manifest.members]
    missing = [table_id for table_id in expected_ids if table_id not in actual_ids]
    extra = [table_id for table_id in actual_ids if table_id not in expected_ids]
    if missing:
        raise _err(
            "E_MANIFEST_MEMBER_MISSING",
            f"manifest is missing contract table(s): {', '.join(missing)}",
            table_id=missing[0],
            field="members",
            path=manifest.path,
        )
    if extra:
        raise _err(
            "E_MANIFEST_MEMBER_EXTRA",
            f"manifest contains table(s) not declared by the contract: {', '.join(extra)}",
            table_id=extra[0],
            field="members",
            path=manifest.path,
        )
    if actual_ids != expected_ids:
        raise _err(
            "E_MANIFEST_MEMBER_ORDER",
            "manifest members are reordered; contract order is "
            f"{expected_ids}, manifest order is {actual_ids}",
            field="members",
            path=manifest.path,
        )

    by_id = {member.table_id: member for member in manifest.members}
    source_mismatches = [
        spec.table_id
        for spec in contract.tables
        if by_id[spec.table_id].source != spec.source
    ]
    if source_mismatches:
        table_id = source_mismatches[0]
        raise _err(
            "E_MANIFEST_MEMBER_SOURCE",
            "manifest source does not match the contract source for table(s): "
            + ", ".join(
                f"{item} (manifest={by_id[item].source!r}, "
                f"contract={contract.table(item).source!r})"
                for item in source_mismatches
            ),
            table_id=table_id,
            field="source",
            path=manifest.path,
        )

    if manifest.contract_hash != contract.normalized_hash:
        raise _err(
            "E_MANIFEST_CONTRACT_MISMATCH",
            "manifest is bound to a different normalized contract hash",
            field="contract_hash",
            path=manifest.path,
        )


def recheck_sources(contract: Contract, manifest: InputManifest) -> None:
    """Re-hash every live source and report all members that no longer match."""

    changed: list[ManifestMember] = []
    for spec in contract.tables:
        member = manifest.member(spec.table_id)
        path = _resolve_source(contract, spec)
        try:
            sha256, size = _hash_file(path)
        except OSError:
            changed.append(member)
            continue
        if sha256 != member.sha256 or size != member.size:
            changed.append(member)
    if changed:
        raise _err(
            "E_MANIFEST_SOURCE_CHANGED",
            f"source file(s) changed during input capture: {_format_members(changed)}",
            table_id=changed[0].table_id,
            field="members",
            path=manifest.path,
        )


@dataclass
class _CapturedMember:
    member: ManifestMember
    data: bytes | None = None
    snapshot_path: Path | None = None

    def read_bytes(self) -> bytes:
        if self.data is not None:
            return self.data
        assert self.snapshot_path is not None
        try:
            raw = self.snapshot_path.read_bytes()
        except OSError as exc:
            raise _err(
                "E_MANIFEST_SNAPSHOT",
                "snapshot file is not readable",
                table_id=self.member.table_id,
                path=self.snapshot_path,
            ) from exc
        digest = hashlib.sha256(raw).hexdigest()
        if digest != self.member.sha256 or len(raw) != self.member.size:
            raise _err(
                "E_MANIFEST_SNAPSHOT_CORRUPT",
                "snapshot bytes do not match the manifest member",
                table_id=self.member.table_id,
                field="sha256",
                path=self.snapshot_path,
            )
        return raw


class CapturedInputSet(AbstractContextManager["CapturedInputSet"]):
    """Verified, immutable-at-capture input bytes for every contract table."""

    def __init__(
        self,
        manifest: InputManifest,
        members: Mapping[str, _CapturedMember],
        *,
        mode: str,
        snapshot_root: Path | None = None,
        snapshot_base: Path | None = None,
    ) -> None:
        self.manifest = manifest
        self.mode = mode
        self._members = dict(members)
        self._snapshot_root = snapshot_root
        self._snapshot_base = snapshot_base

    def read_bytes(self, table_id: str) -> bytes:
        try:
            return self._members[table_id].read_bytes()
        except KeyError as exc:
            raise _err("E_MANIFEST_MEMBER_MISSING", "manifest has no member for table", table_id=table_id) from exc

    def member(self, table_id: str) -> ManifestMember:
        return self._members[table_id].member

    def input_set_record(self) -> dict[str, Any]:
        record = self.manifest.as_record()
        record["mode"] = f"manifest:{self.mode}"
        record["snapshot_base"] = str(self._snapshot_base) if self._snapshot_base else None
        return record

    def close(self) -> None:
        if self._snapshot_root is not None:
            shutil.rmtree(self._snapshot_root, ignore_errors=True)
            self._snapshot_root = None

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


def _read_all_bytes(contract: Contract, manifest: InputManifest) -> dict[str, _CapturedMember]:
    captured: dict[str, _CapturedMember] = {}
    mismatches: list[tuple[ManifestMember, str, int]] = []
    for spec in contract.tables:
        member = manifest.member(spec.table_id)
        path = _resolve_source(contract, spec)
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise _err(
                "E_MANIFEST_SOURCE_READ",
                "cannot read manifest source",
                table_id=spec.table_id,
                path=spec.source,
            ) from exc
        digest = hashlib.sha256(raw).hexdigest()
        if digest != member.sha256 or len(raw) != member.size:
            mismatches.append((member, digest, len(raw)))
            continue
        captured[spec.table_id] = _CapturedMember(member, data=raw)
    if mismatches:
        raise _member_mismatch(mismatches)
    recheck_sources(contract, manifest)
    return captured


def _member_mismatch(mismatches: list[tuple[ManifestMember, str, int]]) -> ManifestError:
    details = "; ".join(
        f"{member.table_id}({member.source}) expected sha256={member.sha256} size={member.size} "
        f"observed sha256={digest} size={size}"
        for member, digest, size in mismatches
    )
    first = mismatches[0][0]
    return _err(
        "E_MANIFEST_MEMBER_CHANGED",
        f"source bytes do not match manifest member(s): {details}",
        table_id=first.table_id,
        field="sha256",
        path=first.source,
    )


def _make_snapshot_dir(snapshot_base: Path | None) -> Path:
    try:
        return Path(tempfile.mkdtemp(prefix="cellcloak-snapshot-", dir=str(snapshot_base) if snapshot_base else None))
    except OSError as exc:
        raise _err("E_MANIFEST_SNAPSHOT", "cannot create temporary snapshot directory", path=snapshot_base) from exc


def _copy_all_snapshots(
    contract: Contract, manifest: InputManifest, snapshot_dir: Path
) -> dict[str, _CapturedMember]:
    captured: dict[str, _CapturedMember] = {}
    mismatches: list[tuple[ManifestMember, str, int]] = []
    try:
        for index, spec in enumerate(contract.tables):
            member = manifest.member(spec.table_id)
            source = _resolve_source(contract, spec)
            target = snapshot_dir / f"{index:04d}-{spec.table_id}.csv"
            digest = hashlib.sha256()
            size = 0
            try:
                with source.open("rb") as reader, target.open("xb") as writer:
                    while True:
                        chunk = reader.read(1024 * 1024)
                        if not chunk:
                            break
                        digest.update(chunk)
                        writer.write(chunk)
                        size += len(chunk)
            except OSError as exc:
                raise _err(
                    "E_MANIFEST_SOURCE_READ",
                    "cannot copy manifest source into the snapshot",
                    table_id=spec.table_id,
                    path=spec.source,
                ) from exc
            observed = digest.hexdigest()
            if observed != member.sha256 or size != member.size:
                mismatches.append((member, observed, size))
                continue
            captured[spec.table_id] = _CapturedMember(member, snapshot_path=target)
        if mismatches:
            raise _member_mismatch(mismatches)
        # The captured set is only accepted after every live source still
        # matches. A replacement during the copy pass is reported by member.
        recheck_sources(contract, manifest)
    except BaseException:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        raise
    return captured


def capture_input_set(
    contract: Contract,
    manifest: InputManifest,
    *,
    mode: str = DEFAULT_INPUT_MODE,
    snapshot_dir: str | Path | None = None,
) -> CapturedInputSet:
    """Verify the manifest and capture bytes for every declared table.

    ``mode="bytes"`` retains all member bytes in memory. ``mode="snapshot"``
    copies them into a private temporary directory and parses only that copy.
    The returned context manager removes temporary files on exit.
    """

    if mode not in INPUT_MODES:
        raise _err("E_MANIFEST_ARGUMENT", f"input mode must be one of {', '.join(INPUT_MODES)}")
    if mode == "bytes" and snapshot_dir is not None:
        raise _err("E_MANIFEST_ARGUMENT", "--snapshot-dir requires snapshot input mode")

    validate_manifest(contract, manifest)
    snapshot_base: Path | None = None
    if snapshot_dir is not None:
        snapshot_base = Path(snapshot_dir).expanduser()
        if not snapshot_base.is_dir():
            raise _err("E_MANIFEST_SNAPSHOT", "snapshot directory is not an existing directory", path=snapshot_base)
        snapshot_base = snapshot_base.resolve()

    if mode == "bytes":
        captured = _read_all_bytes(contract, manifest)
        return CapturedInputSet(manifest, captured, mode=mode)

    snapshot_root = _make_snapshot_dir(snapshot_base)
    try:
        captured = _copy_all_snapshots(contract, manifest, snapshot_root)
    except BaseException:
        shutil.rmtree(snapshot_root, ignore_errors=True)
        raise
    return CapturedInputSet(
        manifest,
        captured,
        mode=mode,
        snapshot_root=snapshot_root,
        snapshot_base=snapshot_base,
    )
