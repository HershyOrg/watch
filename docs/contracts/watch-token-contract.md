# Watch Token Contract v1

## 1. Purpose

이 문서는 Watch framework vocabulary의 canonical source다. Watch로 작성된 코드나 설명에서 공통으로 참조할 수 있는 token kind, field, field type, enum, identity policy를 정의한다.

이 계약이 정의하는 것은 Watch token language다.

- 어떤 Watch token kind가 있는가.
- 각 token이 어떤 field를 가질 수 있는가.
- 각 field가 어떤 type과 required 여부를 가지는가.
- 같은 Watch token instance를 맞출 때 어떤 field가 identity에 참여하는가.
- token identity가 strong인지 weak인지.
- variant가 있는 token의 field constraint가 무엇인가.

이 계약이 정의하지 않는 것도 명확하다.

- artifact schema가 아니다.
- claim envelope가 아니다.
- 비교 알고리즘이 아니다.
- provider-specific extension, source reference, diagnostic, provenance, confidence를 정의하지 않는다.
- 외부 도구의 graph, UI, storage, deployment artifact shape를 정의하지 않는다.

각 도구는 자기 native artifact를 유지한 채 이 계약의 Watch token instance를 표현할 수 있다. Watch는 그 envelope나 저장 형식을 소유하지 않는다.

## 2. Machine Contract

기계 판독 manifest는 다음 파일이다.

- `/home/rlaaudgjs5638/thirdeye/watch/contracts/watch.tokens.v1.json`

현재 schema version은 `watch.tokens.v1`이다. Watch token vocabulary를 바꿀 때는 manifest와 이 문서를 함께 갱신해야 한다.

## 3. Field Types

Scalar field type:

| Type | Meaning |
|---|---|
| `String` | String value such as a name, key, or literal condition. |
| `StringList` | List of string values. |
| `Expression` | Neutral expression value. The representation is implementation-specific. |
| `DurationExpression` | Duration expression such as `time.Second` or `3*time.Minute`. |
| `TypeExpression` | Type expression such as `int` or `shared.WatchValue[float64]`. |
| `FunctionRef` | Function or handler reference. |
| `KeyValueList` | Key/value information list. |

Generic field type:

| Type | Meaning |
|---|---|
| `Enum<T>` | Enum value from enum `T`. |
| `TokenRef<T>` | Reference to another Watch token instance of kind `T`. |
| `TokenRefList<T>` | List of references to Watch token instances of kind `T`. |

`TokenRef<T>` and `TokenRefList<T>` express token reference semantics by type. The contract does not use separate `relationFields`, `valueFields`, or field roles.

## 4. Identity Policy

Each field may declare `identity: true` in the machine contract. The ordered set of identity fields is the matching key for a Watch token instance. Every identity field must be required. `identityStrength` has two values:

- `strong`: the identity can be used as a unique token instance key in normal cases.
- `weak`: the identity is only a grouping or candidate narrowing key.

Identity policy:

| Token | Identity | Strength |
|---|---|---|
| `Watcher` | singleton / no base identity field | `weak` |
| `Manager` | `Name` | `strong` |
| `Cleanup` | `Owner` | `strong` |
| `WatchMachine` | `Owner`, `Name` | `strong` |
| `WatchCall` | `Owner`, `Name` | `strong` |
| `WatchFlow` | `Owner`, `Name` | `strong` |
| `WatchTick` | `Owner`, `Name` | `strong` |
| `Memo` | `Owner`, `Key` | `strong` |
| `ClearMemo` | `Owner`, `Key` | `strong` |
| `ValueStoreAccess` | `Owner`, `Key`, `Operation` | `strong` |
| `MessageDispatch` | `Owner`, `Condition` | `strong` |
| `MessageCase` | `Owner`, `Kind`, `Condition` | `strong` |
| `ControlSignal` | `Owner`, `Signal` | `weak` |

`ControlSignal` is weak because the same Manager can return the same signal from multiple sites. Analyzer-specific context such as source location, return ordinal, or branch context is not part of Watch token identity.

`MessageCase` keeps identity simple by always requiring `Condition`:

- `Kind = Literal`: `Condition` is the literal case value.
- `Kind = Default`: `Condition` is the reserved default branch sentinel `*`.

The `*` value is reserved by this contract only for `MessageCase.Kind = Default`.

## 5. Enums

`ControlSignal` values:

- `None`
- `Stop`
- `Kill`
- `Crash`

`HookKind` values:

- `WatchCall`
- `WatchFlow`
- `WatchTick`
- `Memo`
- `ClearMemo`
- `ValueStoreAccess`
- `UserEnvRead`

`ValueStoreOperation` values:

- `Get`
- `Set`
- `Update`

`MessageCaseKind` values:

- `Literal`
- `Default`

## 6. Tokens

### Watcher

Component token for a Watcher instance and lifecycle root.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Managers` | `TokenRefList<Manager>` | yes | Managers owned by the Watcher. |
| `Name` | `String` | no | Optional display name. |
| `Lifecycle` | `StringList` | no | Lifecycle calls such as Start, Run, or Stop. |

### Manager

Component token for a `Manage` registration unit and managed function.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Name` | `String` | yes | Manager name. |
| `ManagedFunc` | `FunctionRef` | yes | Managed function entry point. |
| `ConfigValues` | `KeyValueList` | no | Immutable configuration values passed to `Manage`; not the canonical UserEnv read path. |
| `Messages` | `TokenRefList<MessageDispatch>` | no | Message dispatch tokens owned by the Manager. |
| `ControlSignals` | `TokenRefList<ControlSignal>` | no | Control signals returned by the Manager. |
| `Cleanup` | `TokenRef<Cleanup>` | no | Cleanup attached to the Manager. |

### Cleanup

Component token for a cleanup function attached to a Manager.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `CleanupFunc` | `FunctionRef` | yes | Cleanup function entry point. |

### WatchMachine

Component token for the observed or updated unit created by watch APIs.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `Name` | `String` | yes | WatchMachine name. |
| `Kind` | `Enum<HookKind>` | yes | API kind that creates or addresses this WatchMachine. |
| `Handler` | `FunctionRef` | no | Update or flow handler entry point. |
| `Tick` | `DurationExpression` | no | Tick interval expression. |
| `InitialValue` | `Expression` | no | Initial value expression. |
| `ValueType` | `TypeExpression` | no | Observed value type expression. |

### WatchCall

Hook token for handler-based WatchMachine registration.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `Name` | `String` | yes | WatchMachine name created or addressed by this hook. |
| `Handler` | `FunctionRef` | yes | WatchCall handler entry point. |
| `Target` | `TokenRef<WatchMachine>` | no | Target WatchMachine token. |
| `InitialValue` | `Expression` | no | Initial value expression. |
| `Tick` | `DurationExpression` | no | Tick interval expression. |

### WatchFlow

Hook token for flow handler-based WatchMachine registration.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `Name` | `String` | yes | WatchMachine name created or addressed by this hook. |
| `Handler` | `FunctionRef` | yes | WatchFlow handler entry point. |
| `Target` | `TokenRef<WatchMachine>` | no | Target WatchMachine token. |
| `InitialValue` | `Expression` | no | Initial value expression. |
| `ValueType` | `TypeExpression` | no | Flow value type expression. |

### WatchTick

Hook token for tick-based WatchMachine registration.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `Name` | `String` | yes | WatchMachine name created or addressed by this hook. |
| `Tick` | `DurationExpression` | yes | Tick interval expression. |
| `Target` | `TokenRef<WatchMachine>` | no | Target WatchMachine token. |

### Memo

Hook token for a memo/cache value created by `Memo`.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `Key` | `String` | yes | Memo key. |
| `ValueType` | `TypeExpression` | no | Memo value type expression. |

### ClearMemo

Hook token for clearing a memo key.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `Key` | `String` | yes | Memo key to clear. |
| `Target` | `TokenRef<Memo>` | no | Memo token cleared by this operation. |

### ValueStoreAccess

Hook token for `GetValue`, `SetValue`, or `UpdateValue`.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `Key` | `String` | yes | Value store key. |
| `Operation` | `Enum<ValueStoreOperation>` | yes | Value store operation kind. |

### UserEnvRead

Hook token for `watch.ReadEnv`. It represents a user-info read from the caller directory fixed `.watch.env` file. The raw value is never part of the token claim or any artifact.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `Key` | `String` | yes | UserEnv key read through `watch.ReadEnv`. |
| `Source` | `String` | yes | Fixed source file name: `.watch.env`. |
| `ValueType` | `TypeExpression` | yes | Always `string`. |

### MessageDispatch

Message token for dispatch by message content or equivalent condition.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Owning Manager. |
| `Condition` | `Expression` | yes | Dispatch condition expression. |
| `Cases` | `TokenRefList<MessageCase>` | no | Message cases belonging to this dispatch. |

### MessageCase

Message token for one literal or default branch inside a MessageDispatch.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<MessageDispatch>` | yes | Owning MessageDispatch. |
| `Kind` | `Enum<MessageCaseKind>` | yes | Message case kind. |
| `Condition` | `String` | yes | Literal case value, or `*` when Kind is Default. |

Semantic constraints:

- `Kind = Literal`: `Condition` is the literal case value.
- `Kind = Default`: `Condition` is `*`.

### ControlSignal

Control token for a managed function control result.

| Field | Type | Required | Meaning |
|---|---|---|---|
| `Owner` | `TokenRef<Manager>` | yes | Manager returning this control signal. |
| `Signal` | `Enum<ControlSignal>` | yes | Control signal kind. |
| `Reason` | `Expression` | no | Reason expression for Stop, Kill, or Crash. |
| `Error` | `Expression` | no | Returned error expression. |

## 7. Token Instance Example

This is a Watch token instance example only. It is not a claim envelope and does not define any artifact shape.

```json
{
  "kind": "WatchCall",
  "identity": {
    "Owner": "Manager:trade",
    "Name": "btc_price"
  },
  "fields": {
    "Owner": "Manager:trade",
    "Name": "btc_price",
    "Handler": "fetchPrice",
    "Tick": "time.Second"
  }
}
```

## 8. Operations

Watch token vocabulary changes must update:

- `/home/rlaaudgjs5638/thirdeye/watch/contracts/watch.tokens.v1.json`
- `/home/rlaaudgjs5638/thirdeye/watch/docs/contracts/watch-token-contract.md`
- `/home/rlaaudgjs5638/thirdeye/watch/contracts/scripts/validate_watch_tokens.py` when validation rules change
