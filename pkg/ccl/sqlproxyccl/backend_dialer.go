// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package sqlproxyccl

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"

	"github.com/jackc/pgproto3/v2"
)

// sslOverlay attempts to upgrade the PG connection to use SSL if a tls.Config
// is specified.
func sslOverlay(conn net.Conn, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig == nil {
		return conn, nil
	}

	var err error
	// Send SSLRequest.
	if err := binary.Write(conn, binary.BigEndian, pgSSLRequest); err != nil {
		return nil, newErrorf(
			codeBackendDown, "sending SSLRequest to target server: %v", err,
		)
	}

	response := make([]byte, 1)
	if _, err = io.ReadFull(conn, response); err != nil {
		return nil,
			newErrorf(codeBackendDown, "reading response to SSLRequest")
	}

	if response[0] != pgAcceptSSLRequest {
		return nil, newErrorf(
			codeBackendRefusedTLS, "target server refused TLS connection",
		)
	}

	outCfg := tlsConfig.Clone()
	return tls.Client(conn, outCfg), nil
}

// relayStartupMsg forwards the start message on the backend connection.
func relayStartupMsg(conn net.Conn, msg *pgproto3.StartupMessage) error {
	if _, err := conn.Write(msg.Encode(nil)); err != nil {
		return newErrorf(
			codeBackendDown, "relaying StartupMessage to target server %v: %v",
			conn.RemoteAddr(), err)
	}
	return nil
}
