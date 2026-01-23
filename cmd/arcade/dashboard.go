package main

import (
	"fmt"
	"time"

	msgbus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/gofiber/fiber/v2"

	"github.com/bsv-blockchain/arcade"
)

// Dashboard provides HTTP handlers for the status dashboard
type Dashboard struct {
	arcade *arcade.Arcade
}

// NewDashboard creates a new dashboard handler
func NewDashboard(a *arcade.Arcade) *Dashboard {
	return &Dashboard{arcade: a}
}

// HandleDashboard renders the status dashboard
func (d *Dashboard) HandleDashboard(c *fiber.Ctx) error {
	peers := d.arcade.GetPeers()
	peerID := d.arcade.GetPeerID()

	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>Arcade Status</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body {
            font-family: 'Courier New', monospace;
            background: #1a1a1a;
            color: #00ff00;
            padding: 20px;
            margin: 0;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #00ff00;
            border-bottom: 2px solid #00ff00;
            padding-bottom: 10px;
        }
        h2 {
            color: #00cc00;
            margin-top: 0;
        }
        .section {
            background: #0d0d0d;
            border: 1px solid #00ff00;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }
        .label {
            color: #808080;
            display: inline-block;
            width: 150px;
        }
        .value {
            color: #00ff00;
            font-weight: bold;
        }
        .peer-list {
            margin-top: 10px;
        }
        .peer {
            background: #1a1a1a;
            border-left: 3px solid #00ff00;
            padding: 10px;
            margin: 5px 0;
        }
        .peer-id {
            color: #00cccc;
            font-size: 0.85em;
        }
        .peer-addr {
            color: #808080;
            font-size: 0.75em;
            margin-left: 20px;
        }
        .status-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%%;
            background: #00ff00;
            margin-right: 10px;
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%%, 100%% { opacity: 1; }
            50%% { opacity: 0.5; }
        }
        .timestamp {
            color: #808080;
            font-size: 0.9em;
            text-align: right;
            margin-top: 20px;
        }
        .self-id {
            color: #ffcc00;
            font-size: 0.9em;
            word-break: break-all;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1><span class="status-indicator"></span>Arcade Status Dashboard</h1>

        <div class="section">
            <h2>P2P Network</h2>
            <div><span class="label">Node ID:</span><span class="self-id">%s</span></div>
            <div><span class="label">Connected Peers:</span><span class="value">%d</span></div>
            <div class="peer-list">
                %s
            </div>
        </div>

        <div class="timestamp">
            Last updated: %s (auto-refresh every 10s)
        </div>
    </div>
</body>
</html>`,
		peerID,
		len(peers),
		renderPeerList(peers),
		time.Now().Format("2006-01-02 15:04:05 MST"),
	)

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.SendString(html)
}

// renderPeerList generates HTML for the peer list
func renderPeerList(peers []msgbus.PeerInfo) string {
	if len(peers) == 0 {
		return `<div style="color: #808080; font-style: italic;">No peers connected</div>`
	}

	html := ""
	for _, peer := range peers {
		name := peer.Name
		if name == "unknown" || name == "" {
			name = "Unknown Peer"
		}

		addrs := ""
		for _, addr := range peer.Addrs {
			addrs += fmt.Sprintf(`<div class="peer-addr">%s</div>`, addr)
		}

		html += fmt.Sprintf(`
			<div class="peer">
				<div><strong>%s</strong></div>
				<div class="peer-id">%s</div>
				%s
			</div>
		`, name, peer.ID, addrs)
	}

	return html
}
