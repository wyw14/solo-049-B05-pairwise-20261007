package api

import "net/http"

func (r *Runtime) listStormRecoveries(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": r.Weather.ListRecoveries()})
}

func (r *Runtime) reconcileStormRecoveries(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, r.Weather.ReconcilePending())
}
