package weather

import "time"

type WindSample struct {
	StationID           string    `json:"station_id"`
	MetersPerSecond     float64   `json:"meters_per_second"`
	GustMetersPerSecond float64   `json:"gust_meters_per_second"`
	DirectionDegrees    float64   `json:"direction_degrees"`
	ObservedAt          time.Time `json:"observed_at"`
}

type StowResult struct {
	OperationID      string `json:"operation_id"`
	AntennaID        string `json:"antenna_id"`
	ChainID          string `json:"chain_id"`
	InhibitConfirmed bool   `json:"inhibit_confirmed"`
	Stowed           bool   `json:"stowed"`
	PassFailed       bool   `json:"pass_failed"`
	PassMissing      bool   `json:"pass_missing,omitempty"`
	RecoveryID       string `json:"recovery_id,omitempty"`
	Error            string `json:"error,omitempty"`
}
