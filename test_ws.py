import asyncio
import websockets

async def test():
    try:
        async with websockets.connect('ws://localhost:5092/ws') as websocket:
            print("Connected to ws://localhost:5092/ws")
            # Send an empty byte and see if it replies or errors
            await websocket.send(b'')
            print("Sent empty frame")
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                print(f"Received: {response}")
            except asyncio.TimeoutError:
                print("No immediate response received (expected, waiting for more audio)")
            except Exception as e:
                print(f"Error reading response: {e}")
    except Exception as e:
        print(f"Connection failed: {e}")

asyncio.run(test())
