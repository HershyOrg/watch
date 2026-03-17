package wm

import (
	"time"

	"github.com/HershyOrg/watch/shared"
)

// LoopHistory는 WatchLoop가 보낸 WatchedNewVar 이벤트를 "적용"한 내역이다.
// WatchMachine의 Subscriber들이 주로 []LoopHistory를 가져간다.
// * LoopHistory는 State가 아니다. Loop의 관측에 의해 Reduce없이도 계속 append됨.
// * 다만 그 자체로 (이론상) 과거 불변의 로그 with TimeStamp므로,
// * State와 같은 층위의 신뢰도를 지닌다.
type LoopHistory []ReducedSnapshot

// ReducedSnapshot은 그 시점에서 Loop가 변수의 UpdateFunc를 관측 후 적용한 결과다.
type ReducedSnapshot struct {
	ReceivedTime time.Time
	//GetHandleErrOrNil은 위해 GetHandle시의 Err도 포함한다.
	// GetHandle자체에서 Err가 나면 UpdateFunc와 Value는 정해지지 않는다.
	GetHandleErrOrNil error
	//ReceivedRawUpdateFunc는 Loop가 받은 함수를 저장한다.
	//함수가 달라질 수 있으므로, 반드시 기록하고, 호출해야 한다.
	RawUpdateFunc RawUpdateFunc
	//ReturnedValue는 이전 값에 UpdateFunc를 적용한 결과이다.
	ReturnedValue shared.RawWatchValue
}
