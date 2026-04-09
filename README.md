# Brain Services

Bridge services that form the Agothe brain's neural bands.

| Service | Port | Description |
|---------|------|-------------|
| memory_bridge | 5555 | Qdrant vector memory interface |
| notion_bridge | 5558 | Notion knowledge base sync |
| brain_visualizer | 5562 | Real-time brain state HUD |
| quantum_bridge | 5564 | Quantum reasoning layer |
| consistency_checker | 5575 | Logical consistency validator |

## Start all services
```bash
python memory_bridge.py &
python notion_bridge.py &
python brain_visualizer_server.py &
python quantum_bridge.py &
python consistency_checker.py &
```
