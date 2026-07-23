// SPDX-License-Identifier: Apache-2.0

// Package export_actr_workload_workload implements every export in the
// asynchronous actr:workload/workload@0.2.0 interface.
package export_actr_workload_workload

import (
	types "wit_component/actr_workload_types"

	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func ok() witTypes.Result[witTypes.Unit, types.ActrError] {
	return witTypes.Ok[witTypes.Unit, types.ActrError](witTypes.Unit{})
}

// Dispatch echoes the raw request payload while accepting the invocation
// context explicitly, as required by the concurrent V2 ABI.
func Dispatch(
	envelope types.RpcEnvelope,
	_ types.InvocationCtx,
) witTypes.Result[[]uint8, types.ActrError] {
	response := make([]byte, 0, len("echo: ")+len(envelope.Payload))
	response = append(response, "echo: "...)
	response = append(response, envelope.Payload...)
	return witTypes.Ok[[]uint8, types.ActrError](response)
}

func OnStart(_ types.InvocationCtx) witTypes.Result[witTypes.Unit, types.ActrError] {
	return ok()
}

func OnReady(_ types.InvocationCtx) witTypes.Result[witTypes.Unit, types.ActrError] {
	return ok()
}

func OnStop(_ types.InvocationCtx) witTypes.Result[witTypes.Unit, types.ActrError] {
	return ok()
}

func OnError(
	_ types.ErrorEvent,
	_ types.InvocationCtx,
) witTypes.Result[witTypes.Unit, types.ActrError] {
	return ok()
}

func OnSignalingConnecting(_ types.InvocationCtx) {}

func OnSignalingConnected(_ types.InvocationCtx) {}

func OnSignalingDisconnected(_ types.InvocationCtx) {}

func OnWebsocketConnecting(_ types.PeerEvent, _ types.InvocationCtx) {}

func OnWebsocketConnected(_ types.PeerEvent, _ types.InvocationCtx) {}

func OnWebsocketDisconnected(_ types.PeerEvent, _ types.InvocationCtx) {}

func OnWebrtcConnecting(_ types.PeerEvent, _ types.InvocationCtx) {}

func OnWebrtcConnected(_ types.PeerEvent, _ types.InvocationCtx) {}

func OnWebrtcDisconnected(_ types.PeerEvent, _ types.InvocationCtx) {}

func OnCredentialRenewed(_ types.CredentialEvent, _ types.InvocationCtx) {}

func OnCredentialExpiring(_ types.CredentialEvent, _ types.InvocationCtx) {}

func OnMailboxBackpressure(_ types.BackpressureEvent, _ types.InvocationCtx) {}

func OnDataChunk(
	_ types.DataChunk,
	_ types.ActrId,
	_ types.InvocationCtx,
) witTypes.Result[witTypes.Unit, types.ActrError] {
	return ok()
}
