import asyncio
import uuid
from typing import Dict
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from streaming_vad import StreamingVAD
from batchworker import transcription_queue, condition, results

router = APIRouter()

# Global map of session_id -> events_ws
active_sessions: Dict[str, WebSocket] = {}
# Global map of audio chunk path -> session_id
chunk_owner: Dict[str, str] = {}

class ConnectionManager:
    async def get_ws(self, session_id: str) -> WebSocket | None:
        return active_sessions.get(session_id)

manager = ConnectionManager()


@router.websocket("/ws/events/{session_id}")
async def ws_events(websocket: WebSocket, session_id: str):
    await websocket.accept()
    active_sessions[session_id] = websocket
    print(f"[WS SERVER] Event connection accepted for session: {session_id}")
    try:
        # Keep connection open, client just listens
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        print(f"[WS SERVER] Event connection closed for session: {session_id}")
        active_sessions.pop(session_id, None)
    except Exception as e:
        print(f"[WS SERVER] Event connection error: {e}")
        active_sessions.pop(session_id, None)


@router.websocket("/ws/audio/{session_id}")
async def ws_audio(ws: WebSocket, session_id: str):
    await ws.accept()
    print(f"[WS SERVER] Audio connection accepted for session: {session_id}")
    vad = StreamingVAD()

    async def producer():
        """push chunks into the global transcription queue"""
        try:
            chunks_received = 0
            while True:
                frame = await ws.receive_bytes()
                chunks_received += 1
                if chunks_received % 100 == 0:
                    print(f"[WS SERVER] Received {chunks_received} chunks of audio for session {session_id}.")
                    
                paths, events = vad.feed(frame)
                
                # Push events to the events socket if it exists
                events_ws = await manager.get_ws(session_id)
                if events_ws and events:
                    for ev in events:
                        await events_ws.send_json(ev)

                for chunk_path in paths:
                    # Map this chunk back to the session so the consumer knows who to notify
                    chunk_owner[chunk_path] = session_id
                    await transcription_queue.put(chunk_path)
                    
        except WebSocketDisconnect:
            print(f"[WS SERVER] Audio client disconnected session: {session_id}")
        except Exception as e:
            print(f"[WS SERVER] Error in audio producer: {e}")

    async def consumer():
        """stream results back as soon as they’re ready for this session"""
        try:
            while True:
                async with condition:
                    await condition.wait()
                
                flushed = []
                events_ws = await manager.get_ws(session_id)
                
                # Iterate over results, fetching only those belonging to this session
                for p, txt in list(results.items()):
                    if chunk_owner.get(p) == session_id:
                        if events_ws:
                            await events_ws.send_json({"text": txt})
                        flushed.append(p)
                
                for p in flushed:
                    results.pop(p, None)
                    chunk_owner.pop(p, None)
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[WS SERVER] Error in audio consumer: {e}")

    prod_task = asyncio.create_task(producer())
    cons_task = asyncio.create_task(consumer())
    
    done, pending = await asyncio.wait(
        [prod_task, cons_task],
        return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
