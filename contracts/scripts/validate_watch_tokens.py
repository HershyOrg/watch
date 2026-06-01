#!/usr/bin/env python3
import json
import re
from pathlib import Path


CONTRACT_ROOT = Path(__file__).resolve().parents[1]
WATCH_ROOT = CONTRACT_ROOT.parent
MANIFEST = CONTRACT_ROOT / "watch.tokens.v1.json"
DOC = WATCH_ROOT / "docs" / "contracts" / "watch-token-contract.md"

EXPECTED_SCHEMA_VERSION = "watch.tokens.v1"
EXPECTED_GROUPS = {"Component", "Hook", "Message", "Control"}
EXPECTED_IDENTITY_STRENGTHS = {"strong", "weak"}
EXPECTED_CONTROL_SIGNALS = ["None", "Stop", "Kill", "Crash"]
EXPECTED_MESSAGE_CASE_KINDS = ["Literal", "Default"]
FORBIDDEN_KEYS = {
    "relationFields",
    "valueFields",
    "roles",
    "identityFields",
    "variantIdentityFields",
    "identityWhen",
    "claimID",
    "provider",
    "artifactElement",
    "sourceRefs",
    "diagnostics",
    "provenance",
    "confidence",
}
GENERIC_RE = re.compile(r"^(Enum|TokenRef|TokenRefList)<([A-Za-z][A-Za-z0-9]*)>$")


def fail(message: str) -> None:
    raise SystemExit(message)


def main() -> None:
    data = json.loads(MANIFEST.read_text(encoding="utf-8"))
    doc = DOC.read_text(encoding="utf-8")

    if data.get("schemaVersion") != EXPECTED_SCHEMA_VERSION:
        fail(f"schemaVersion must be {EXPECTED_SCHEMA_VERSION}")

    find_forbidden_keys(data)

    field_types = data.get("fieldTypes")
    generic_field_types = data.get("genericFieldTypes")
    enums = data.get("enums")
    tokens = data.get("tokens")
    if not isinstance(field_types, dict) or not field_types:
        fail("fieldTypes must be a non-empty object")
    if not isinstance(generic_field_types, dict) or not generic_field_types:
        fail("genericFieldTypes must be a non-empty object")
    if not isinstance(enums, dict) or not enums:
        fail("enums must be a non-empty object")
    if not isinstance(tokens, dict) or not tokens:
        fail("tokens must be a non-empty object")

    if enums.get("ControlSignal") != EXPECTED_CONTROL_SIGNALS:
        fail(f"ControlSignal enum mismatch: {enums.get('ControlSignal')!r}")
    if enums.get("MessageCaseKind") != EXPECTED_MESSAGE_CASE_KINDS:
        fail(f"MessageCaseKind enum mismatch: {enums.get('MessageCaseKind')!r}")

    for enum_name, values in enums.items():
        if not isinstance(values, list) or not values:
            fail(f"enum {enum_name!r} must be a non-empty list")
        for value in values:
            if not isinstance(value, str) or not value:
                fail(f"enum {enum_name!r} contains an invalid value {value!r}")
            if f"`{value}`" not in doc:
                fail(f"enum value {enum_name}.{value} is missing from documentation")

    for token_name, token in tokens.items():
        validate_token(token_name, token, field_types, enums, tokens, doc)

    validate_message_case(tokens, doc)
    print(f"ok: {len(tokens)} tokens, {len(enums)} enums")


def find_forbidden_keys(value, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in FORBIDDEN_KEYS:
                fail(f"forbidden key {key!r} found at {path}")
            find_forbidden_keys(child, f"{path}.{key}")
    elif isinstance(value, list):
        for i, child in enumerate(value):
            find_forbidden_keys(child, f"{path}[{i}]")


def validate_token(token_name, token, field_types, enums, tokens, doc) -> None:
    if f"### {token_name}" not in doc:
        fail(f"token {token_name!r} is missing from documentation")
    if not isinstance(token, dict):
        fail(f"token {token_name!r} must be an object")

    group = token.get("group")
    if group not in EXPECTED_GROUPS:
        fail(f"token {token_name!r} has invalid group {group!r}")

    identity_strength = token.get("identityStrength")
    if identity_strength not in EXPECTED_IDENTITY_STRENGTHS:
        fail(f"token {token_name!r} has invalid identityStrength {identity_strength!r}")

    fields = token.get("fields")
    if not isinstance(fields, dict) or not fields:
        fail(f"token {token_name!r} must declare a non-empty fields object")

    for field_name, field in fields.items():
        validate_field(token_name, field_name, field, field_types, enums, tokens)

    identity_fields = [
        field_name
        for field_name, field in fields.items()
        if field.get("identity") is True
    ]
    for field_name in identity_fields:
        if fields[field_name].get("required") is not True:
            fail(f"identity field {token_name}.{field_name} must be required")
    if not identity_fields:
        if identity_strength != "weak" or token.get("singleton") is not True:
            fail(f"token {token_name!r} must have identity fields or be a weak singleton")


def validate_field(token_name, field_name, field, field_types, enums, tokens) -> None:
    if not isinstance(field, dict):
        fail(f"field {token_name}.{field_name} must be an object")
    if "type" not in field:
        fail(f"field {token_name}.{field_name} is missing type")
    if "required" not in field:
        fail(f"field {token_name}.{field_name} is missing required")
    if not isinstance(field["required"], bool):
        fail(f"field {token_name}.{field_name}.required must be boolean")
    if "identity" in field and not isinstance(field["identity"], bool):
        fail(f"field {token_name}.{field_name}.identity must be boolean")

    field_type = field["type"]
    if field_type in field_types:
        return

    match = GENERIC_RE.match(field_type)
    if not match:
        fail(f"field {token_name}.{field_name} uses unknown type {field_type!r}")

    generic, target = match.groups()
    if generic == "Enum":
        if target not in enums:
            fail(f"field {token_name}.{field_name} references unknown enum {target!r}")
        return
    if target not in tokens:
        fail(f"field {token_name}.{field_name} references unknown token {target!r}")


def validate_message_case(tokens, doc: str) -> None:
    token = tokens.get("MessageCase")
    if not isinstance(token, dict):
        fail("MessageCase token is missing")
    if token.get("defaultCondition") != "*":
        fail("MessageCase defaultCondition must be '*'")
    if "`Condition` is `*`" not in doc and "`Condition`은 `*`" not in doc:
        fail("MessageCase default condition sentinel must be documented")
    fields = token.get("fields", {})
    for field_name in ["Owner", "Kind", "Condition"]:
        field = fields.get(field_name)
        if not isinstance(field, dict):
            fail(f"MessageCase.{field_name} is missing")
        if field.get("required") is not True:
            fail(f"MessageCase.{field_name} must be required")
        if field.get("identity") is not True:
            fail(f"MessageCase.{field_name} must be an identity field")


if __name__ == "__main__":
    main()
