### Commit Policy
- Commit Author는 rlaau뿐이다.
- Codex의 공동 저작 표시를 허용하지 않는다.


### Test Policy

**모든 테스트 코드는 `test/` 디렉토리 하위에 위치해야 한다.**

- 소스 패키지(`manager/`, `watch/`, `util/` 등)에 `*_test.go` 파일을 직접 두지 않는다.
- 테스트는 외부 테스트 패키지(external test package)로 작성하여 public API만 테스트한다.
- private 접근이 필요한 경우, 소스에 exported getter/wrapper를 최소한으로 추가한다.
  - 예: `Reducer.Reduce()`, `Reducer.ReduceDriven()`, `Watcher.GetManager()`, `Watcher.IsRunning()`

```bash
# 전체 테스트 실행
go test ./test/... -v -timeout 10m

# 특정 테스트 패키지 실행
go test ./test/managertest/ -v
go test ./test/watchtest/ -v -timeout 10m
```

### Contract Policy

Watch token vocabulary changes must update the canonical contract files together:

- `contracts/watch.tokens.v1.json`
- `docs/contracts/watch-token-contract.md`
- `contracts/scripts/validate_watch_tokens.py` if validation rules change
