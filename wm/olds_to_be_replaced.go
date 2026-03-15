package wm

import (
	"fmt"
	"time"

	"github.com/HershyOrg/watch/shared"
)

//! 이 파일의 타입-구조체-함수들은 삭제 대상임
//! 지금 당장은 다른 패키지에서 참조되고 있어 수정하지 않음

// DELETED_GetComputationFunc returns the RawVarUpdateFunc, a skipSignal flag
// (false by default; set to true if you want to skip), and an error.
// ! 삭제 예정
type DELETED_GetComputationFunc func() (varUpdateFunc DELETED_RawVarUpdateFunc, skipSignal bool, err error)


// VarUpdateFunc is a generic function that updates a variable's state.
// It receives the previous value of type T and returns the next value and an error.
// ! 삭제 예정
type DELETED_VarUpdateFunc[T any] func(prev T) (next T, err error)

// DELETED_RawVarUpdateFunc is the internal non-generic version used by VarSig.
// It receives the previous RawHershValue and returns the next RawHershValue and an error.
// ! 삭제 예정
type DELETED_RawVarUpdateFunc func(prev shared.RawWatchValue) (next shared.RawWatchValue)

// DELETED_VarSig represents a change in a watched variable's state.
// ! 제거 예정
// ! 앞으로 VarSig라는 개념은 해당 프로젝트에 존재하지 않음
// ! 해당 개념은 manager.NewSigAppend와 VarHistory로 대체됨
type DELETED_VarSig struct {
	ReceivedTime  time.Time
	TargetVarName string
	//! 수정 예정
	GetComputeFuncErrOrGetChanErr error     // GetComputeFunc 실행 중 발생한 에러
	SourceType                    WatchType // "Call" or "Flow" 구분
	//! 삭제 예정
	DELETED_VarUpdateFunc DELETED_RawVarUpdateFunc // Function to compute the next state (internal raw version)
	//! 삭제 예정
	DELETED_ISStateIndependent bool // If true, only last signal matters; if false, apply sequentially
}

func (s *DELETED_VarSig) Priority() shared.SignalPriority {
	return shared.PriorityVar
}

func (s *DELETED_VarSig) CreatedAt() time.Time {
	return s.ReceivedTime
}

func (s *DELETED_VarSig) String() string {
	typeStr := "dependent"
	if s.DELETED_ISStateIndependent {
		typeStr = "independent"
	}
	return fmt.Sprintf("VarSig{var=%s, type=%s, time=%s}",
		s.TargetVarName, typeStr, s.ReceivedTime.Format(time.RFC3339))
}
