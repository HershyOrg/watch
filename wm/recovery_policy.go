package wm

// PrescribeFunc는 문제 발생 시 "처방전"을 지시함.
// 리커버리 판단에 쓰임.
// 미약한 경우 WatchLoop를 잠시 자게 하고,
// 에러 연속열이 길 시, WatchLoop를 다시 Start하며
// 최종 실패 시 WatchLoop를 Crash로 전이시킴.
// 단, Call과 Flow의 구조는 같음.
// Call이 Sleep처방이 효과가 있듯, Flow도 Sleep효과 있음
// 대신 Flow의 Sleep은, 그 시간동안 들어오는 모든 에러 무시하는 것
// Call처럼 진짜 Sleep은 못하지만, 대신 Sleep했다 치고 에러 무시하는 것임.
// => Chan은 특정 혼잡 구간에 에러 뱉을 수 있기 때문임.
// 리커버 이펙트가 필요 시 tryRecoverOrNil에 TryRecover를 지시하고,
// 리커버 필요 x시 nil리턴함.
type PrescribeFunc func(varHistory VarReducedHistory) (state LoopState, tryRecoverOrNil *TryRecoverLoop)
