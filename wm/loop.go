package wm

// WatchLoopInterface는 WatchLoop가 해야 할 일에 대한 디자인이다.
type WatchLoopInterface interface {
	// Start시 WatchLoop는 Handle에 따라,
	// (==tick마다 GetUpdateFunc호출 or chan으로 UpdateFunc받기) 값을 받는다.
	// (만약 GetHandle자체에서 에러 날 시, 이를 LoopGetErrFromGetHandle로 송신한다.)
	//추신 1: Handle을 얻고, UpdateFunc를 쓸 때, 각각 ctxConfig에 따른 적절 ctx를 주입한다.
	//추신 2: UpdateFunc를 얻는 것의 실패는 UpdateFunc의 리턴값에 표현되므로 이는 걱정하지 말자.
	// 이후 VarReducedHistory에 UpdateFunc를 적용해 Value,skip을 얻는다.
	// skip==true시
	// 1) VarReducedHistory에 Append하지 않으며
	// 2) NewSigAppend로 구독자에게 보고하지도 않는다.
	// skip==false시
	// 1) WatchMachine의 VarReducedHistory를 통해 이번에 추가할 VarSnapshot을 만든다.
	// 2) VarReducedHistory에 그 VarSnapshot을 추가한다.
	// (에러와 상관없이 반드시 저장한다. 그래야 Mgr측에서 error꺼내서, 사용자 로직 따른 명시적 에러 처리 가능하다.)
	// (굳이 로깅을 할 필요는 없다. VarReducedHistory가 그 자체로 로그기 때문이다.)
	// 3) 추가 후 체크: 만약 Snapshot에 에러가 있다면 LoopGotErrFromUpdateFunc를 Loop에 송신한다.
	// 4) 이후 에러에 관계없이, NewSigAppend로 구독자에게 보고한다.
	// (그래야 구독자도 새 값이건, 새 에러건, 추가되었음 인지한다.)
	// (에러 값도 보고하는게 핵심임. 그래야 Mgr가 MangedFunc실행 시 에러 값을 인지해서 사용자 로직 따른 명시적 에러 처리가 가능하다.)
	Start(ctx RunContext) error
}
