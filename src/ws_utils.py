import json

# Lista WebSocket attivi
clients = []

# Funzione per notificare i client
async def notify_clients(doc: dict):
    to_remove = []
    for ws in clients:
        try:
            await ws.send_text(json.dumps({"event": "document_ready", "doc": doc}))
        except:
            to_remove.append(ws)
    for ws in to_remove:
        clients.remove(ws)