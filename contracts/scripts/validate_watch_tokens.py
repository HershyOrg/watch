#!/usr/bin/env python3
import json
import re
from pathlib import Path


CONTRACT_ROOT = Path(__file__).resolve().parents[1]
WATCH_ROOT = CONTRACT_ROOT.parent
MANIFEST = CONTRACT_ROOT / 'watch.tokens.v1.json'
DOC = WATCH_ROOT / 'docs' / 'contracts' / 'watch-token-contract.md'

EXPECTED_SCHEMA_VERSION = 'watch.tokens.v1'
EXPECTED_GROUPS = {'Component', 'Hook', 'Message', 'Control'}
EXPECTED_CONTROL_SIGNALS = ['None', 'Stop', 'Kill', 'Crash']
EXPECTED_MESSAGE_CASE_KINDS = ['Literal', 'Default']
FORBIDDEN_KEYS = {
    'relationFields',
    'valueFields',
    'roles',
    'identity',
    'identityStrength',
    'identityFields',
    'variantIdentityFields',
    'identityWhen',
    'singleton',
    'claimID',
    'provider',
    'artifactElement',
    'sourceRefs',
    'diagnostics',
    'provenance',
    'confidence',
}
RESERVED_ID_FIELD_NAMES = {'id', 'Id', 'ID'}
GENERIC_RE = re.compile('^(Enum|TokenRef|TokenRefList)<([A-Za-z][A-Za-z0-9]*)>' + chr(36))


def fail(message: str) -> None:
    raise SystemExit(message)


def main() -> None:
    data = json.loads(MANIFEST.read_text(encoding='utf-8'))
    doc = DOC.read_text(encoding='utf-8')

    if data.get('schemaVersion') != EXPECTED_SCHEMA_VERSION:
        fail('schemaVersion must be ' + EXPECTED_SCHEMA_VERSION)

    validate_contract_policy(data.get('contractPolicy'))
    find_forbidden_keys(data)

    field_types = data.get('fieldTypes')
    generic_field_types = data.get('genericFieldTypes')
    enums = data.get('enums')
    tokens = data.get('tokens')
    if not isinstance(field_types, dict) or not field_types:
        fail('fieldTypes must be a non-empty object')
    if not isinstance(generic_field_types, dict) or not generic_field_types:
        fail('genericFieldTypes must be a non-empty object')
    if not isinstance(enums, dict) or not enums:
        fail('enums must be a non-empty object')
    if not isinstance(tokens, dict) or not tokens:
        fail('tokens must be a non-empty object')

    if enums.get('ControlSignal') != EXPECTED_CONTROL_SIGNALS:
        fail('ControlSignal enum mismatch: ' + repr(enums.get('ControlSignal')))
    if enums.get('MessageCaseKind') != EXPECTED_MESSAGE_CASE_KINDS:
        fail('MessageCaseKind enum mismatch: ' + repr(enums.get('MessageCaseKind')))

    tick = chr(96)
    for enum_name, values in enums.items():
        if not isinstance(values, list) or not values:
            fail('enum ' + repr(enum_name) + ' must be a non-empty list')
        for value in values:
            if not isinstance(value, str) or not value:
                fail('enum ' + repr(enum_name) + ' contains an invalid value ' + repr(value))
            if tick + value + tick not in doc:
                fail('enum value ' + enum_name + '.' + value + ' is missing from documentation')

    for token_name, token in tokens.items():
        validate_token(token_name, token, field_types, enums, tokens, doc)

    validate_message_case(tokens, doc)
    print('ok: ' + str(len(tokens)) + ' tokens, ' + str(len(enums)) + ' enums')


def validate_contract_policy(policy) -> None:
    if not isinstance(policy, dict):
        fail('contractPolicy must be an object')
    if policy.get('definesCanonicalID') is not False:
        fail('contractPolicy.definesCanonicalID must be false')
    if policy.get('definesInstanceIdentity') is not False:
        fail('contractPolicy.definesInstanceIdentity must be false')


def find_forbidden_keys(value, path: str = chr(36)) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in FORBIDDEN_KEYS:
                fail('forbidden key ' + repr(key) + ' found at ' + path)
            find_forbidden_keys(child, path + '.' + key)
    elif isinstance(value, list):
        for i, child in enumerate(value):
            find_forbidden_keys(child, path + '[' + str(i) + ']')


def validate_token(token_name, token, field_types, enums, tokens, doc) -> None:
    if '### ' + token_name not in doc:
        fail('token ' + repr(token_name) + ' is missing from documentation')
    if not isinstance(token, dict):
        fail('token ' + repr(token_name) + ' must be an object')

    group = token.get('group')
    if group not in EXPECTED_GROUPS:
        fail('token ' + repr(token_name) + ' has invalid group ' + repr(group))

    fields = token.get('fields')
    if not isinstance(fields, dict) or not fields:
        fail('token ' + repr(token_name) + ' must declare a non-empty fields object')

    for field_name, field in fields.items():
        if field_name in RESERVED_ID_FIELD_NAMES:
            fail('field ' + token_name + '.' + field_name + ' must not define a canonical id')
        validate_field(token_name, field_name, field, field_types, enums, tokens)


def validate_field(token_name, field_name, field, field_types, enums, tokens) -> None:
    if not isinstance(field, dict):
        fail('field ' + token_name + '.' + field_name + ' must be an object')
    if 'type' not in field:
        fail('field ' + token_name + '.' + field_name + ' is missing type')
    if 'required' not in field:
        fail('field ' + token_name + '.' + field_name + ' is missing required')
    if not isinstance(field['required'], bool):
        fail('field ' + token_name + '.' + field_name + '.required must be boolean')

    field_type = field['type']
    if field_type in field_types:
        return

    match = GENERIC_RE.match(field_type)
    if not match:
        fail('field ' + token_name + '.' + field_name + ' uses unknown type ' + repr(field_type))

    generic, target = match.groups()
    if generic == 'Enum':
        if target not in enums:
            fail('field ' + token_name + '.' + field_name + ' references unknown enum ' + repr(target))
        return
    if target not in tokens:
        fail('field ' + token_name + '.' + field_name + ' references unknown token ' + repr(target))


def validate_message_case(tokens, doc: str) -> None:
    token = tokens.get('MessageCase')
    if not isinstance(token, dict):
        fail('MessageCase token is missing')
    if token.get('defaultCondition') != '*':
        fail('MessageCase defaultCondition must be star')
    tick = chr(96)
    if tick + 'Condition' + tick + ' is ' + tick + '*' + tick not in doc and tick + 'Condition' + tick + '은 ' + tick + '*' + tick not in doc:
        fail('MessageCase default condition sentinel must be documented')
    fields = token.get('fields', {})
    for field_name in ['Owner', 'Kind', 'Condition']:
        field = fields.get(field_name)
        if not isinstance(field, dict):
            fail('MessageCase.' + field_name + ' is missing')
        if field.get('required') is not True:
            fail('MessageCase.' + field_name + ' must be required')


if __name__ == '__main__':
    main()
