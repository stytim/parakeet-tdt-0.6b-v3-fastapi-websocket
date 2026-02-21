from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from streaming_vad import StreamingVAD
from batchworker import transcription_queue, condition, results
import asyncio

router = APIRouter()

@router.websocket("/ws")
async def ws_asr(ws: WebSocket):
    await ws.accept()
    vad = StreamingVAD()

    async def producer():
        """push chunks into the global transcription queue"""
        try:
            chunks_received = 0
            while True:
                frame = await ws.receive_bytes()
                chunks_received += 1
                if chunks_received % 100 == 0:
                    print(f"[WS SERVER] Received {chunks_received} chunks of audio from client.")
                    
                paths = vad.feed(frame)
                if paths:
                    print(f"[WS SERVER] VAD emitted {len(paths)} paths: {paths}")
                for chunk in paths:
                    await transcription_queue.put(chunk)
                    await ws.send_json({"status": "queued"})
        except WebSocketDisconnect:
            print("[WS SERVER] Client disconnected.")
        except Exception as e:
            print(f"[WS SERVER] Error in producer: {e}")

    async def consumer():
        """stream results back as soon as they’re ready"""
        try:
            while True:
                async with condition:
                    await condition.wait()          
                flushed = []
                for p, txt in list(results.items()):
                    await ws.send_json({"text": txt})
                    flushed.append(p)
                for p in flushed:
                    results.pop(p, None)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[WS SERVER] Error in consumer: {e}")

    prod_task = asyncio.create_task(producer())
    cons_task = asyncio.create_task(consumer())
    
    done, pending = await asyncio.wait(
        [prod_task, cons_task],
        return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
