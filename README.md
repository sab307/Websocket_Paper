# Websocket_Paper
Websocket testing for Paper.
 ```
 cd go_relay_websocket
 go run . --port 8080 --web-root ../web-client_websocket/
 cd python_client_websocket 
 python3 main.py --data ws://localhost:8080/ws/data --topic /cmd_vel --format json|binary 
 ```