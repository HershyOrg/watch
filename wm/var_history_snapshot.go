package wm

import (
	"time"

	"github.com/HershyOrg/watch/shared"
)

// VarReducedHistory는 WatchLoop가 보낸 WatchedNewVar 이벤트를 "적용"한 내역이다.
// WatchMachine의 Subscriber들이 주로 []VarReducedHistory를 가져간다.
// * VarReducedHistory는 State가 아니다.
// * 다만 그 자체로 불변의 로그 with TimeStamp므로,
// * State와 같은 층위의 신뢰도를 지닌다.
type VarReducedHistory []VarSnapshot

// VarSnapshot은 그 시점에서의 Var의 값임.
type VarSnapshot struct {
	ReceivedTime time.Time
	//ReceivedRawUpdateFunc는 Loop가 받은 함수를 저장한다.
	//함수가 달라질 수 있으므로, 반드시 기록하고, 호출해야 한다.
	RawUpdateFunc RawUpdateFunc
	//ReturnedValue는 이전 값에 UpdateFunc를 적용한 결과이다.
	ReturnedValue shared.RawWatchValue
}
