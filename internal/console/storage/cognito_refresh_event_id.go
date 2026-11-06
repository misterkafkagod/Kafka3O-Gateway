package storage

import "strconv"

func cognitoRefreshEventID(sessionID string, generation int64) string {
	return sessionID + ".refresh." + strconv.FormatInt(generation, 10)
}
