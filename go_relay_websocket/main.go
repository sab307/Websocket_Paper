package main

/*
WebSocket Data Relay (transport-specific build)
===============================================

This binary only operates the WebSocket data hub. The browser opens a
WebSocket to /ws/data?role=browser; Python opens one to /ws/data?role=python;
the hub forwards frames between them as a dumb byte pump. The teleop wire
protocol (binary or JSON) is end-to-end browser↔Python; this server never
inspects it.

It deliberately does NOT serve /ws/signal or /wt. If you need those, use
the sibling go_relay_webrtc/ or go_relay_webtransport/ binary.

Run:
  go run . --port 8443 --tls-cert ../certs/cert.pem --tls-key ../certs/key.pem
  # or plaintext for dev:
  go run . --port 8080 --web-root ../web-client_websocket
*/

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	dataHub.mu.RLock()
	pythonOK := dataHub.python != nil
	browserOK := dataHub.browser != nil
	dataHub.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":            "ok",
		"python_connected":  pythonOK,
		"browser_connected": browserOK,
	})
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	dataHub.mu.RLock()
	defer dataHub.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"type":              "websocket",
		"mode":              "websocket",
		"python_connected":  dataHub.python != nil,
		"browser_connected": dataHub.browser != nil,
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	fPort := flag.String("port", envOr("PORT", "8443"), "HTTPS/WSS listen port (env: PORT)")
	fHTTPPort := flag.String("http-port", envOr("HTTP_PORT", "8080"), "HTTP→HTTPS redirect port (TLS only)")
	fCert := flag.String("tls-cert", envOr("TLS_CERT", ""), "TLS certificate PEM (env: TLS_CERT)")
	fKey := flag.String("tls-key", envOr("TLS_KEY", ""), "TLS private key PEM (env: TLS_KEY)")
	fWebRoot := flag.String("web-root", "../web-client_websocket/Websocket_Paper", "Directory to serve as the web root")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws/data", handleData)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/status", handleStatus)
	mux.Handle("/", http.FileServer(http.Dir(*fWebRoot)))
	handler := corsMiddleware(mux)

	secure := *fCert != "" && *fKey != ""
	ws := "WS "
	scheme := "http"
	if secure {
		ws = "WSS"
		scheme = "https"
	}

	fmt.Println()
	fmt.Printf("  Transport : WEBSOCKET (data hub)%s\n", tlsBanner(secure))
	fmt.Printf("  %-9s : %s://localhost:%s\n", schemeLabel(secure), scheme, *fPort)
	if secure {
		fmt.Printf("  Cert      : %s\n", *fCert)
		fmt.Printf("  Key       : %s\n", *fKey)
	}
	fmt.Printf("  Web root  : %s\n", *fWebRoot)
	fmt.Println()
	fmt.Printf("  %s  /ws/data?role=python     Python peer (data hub)\n", ws)
	fmt.Printf("  %s  /ws/data?role=browser    Browser peer (data hub)\n", ws)
	fmt.Println("  GET  /health                  Health check")
	fmt.Println("  GET  /status                  Status JSON")
	fmt.Println()
	if !secure {
		fmt.Println("  Tip: add --tls-cert and --tls-key to enable HTTPS/WSS")
		fmt.Println()
	}

	if secure {
		go startHTTPRedirect(*fHTTPPort, *fPort)
		log.Fatal(http.ListenAndServeTLS(":"+*fPort, *fCert, *fKey, handler))
	} else {
		log.Fatal(http.ListenAndServe(":"+*fPort, handler))
	}
}

func startHTTPRedirect(httpPort, httpsPort string) {
	redirect := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		target := "https://" + host
		if httpsPort != "443" {
			target += ":" + httpsPort
		}
		target += r.URL.RequestURI()
		http.Redirect(w, r, target, http.StatusMovedPermanently)
	})
	fmt.Printf("  HTTP redirect  :%s → HTTPS :%s\n\n", httpPort, httpsPort)
	if err := http.ListenAndServe(":"+httpPort, redirect); err != nil {
		log.Printf("HTTP redirect listener error: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func tlsBanner(secure bool) string {
	if secure {
		return "   (TLS: HTTPS / WSS)"
	}
	return "   (no TLS — development only)"
}

func schemeLabel(secure bool) string {
	if secure {
		return "HTTPS"
	}
	return "HTTP"
}
