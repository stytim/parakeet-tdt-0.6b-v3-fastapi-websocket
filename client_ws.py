import asyncio
import websockets
import pyaudio
import json
import sys
import argparse
import uuid

# Audio configuration required by Parakeet (16 kHz, 16-bit PCM, Mono)
FORMAT = pyaudio.paInt16
CHANNELS = 1
RATE = 16000
CHUNK = 512  # 32ms frames at 16kHz

async def audio_handler(debug=False):
    # Generate a unique session ID for this client instance
    session_id = str(uuid.uuid4())
    events_uri = f"ws://localhost:5092/ws/events/{session_id}"
    audio_uri = f"ws://localhost:5092/ws/audio/{session_id}"
    
    print("Initializing microphone...")
    try:
        p = pyaudio.PyAudio()
        stream = p.open(format=FORMAT,
                        channels=CHANNELS,
                        rate=RATE,
                        input=True,
                        frames_per_buffer=CHUNK)
        print("Microphone initialized successfully.")
    except Exception as e:
        print(f"❌ Failed to open microphone: {e}")
        print("macOS users: Make sure your terminal has Microphone permissions in System Settings > Privacy & Security.")
        return
    
    print(f"Connecting Event socket to {events_uri}...")
    print(f"Connecting Audio socket to {audio_uri}...")
    try:
        # Open both WebSocket connections concurrently
        async with websockets.connect(events_uri) as events_ws, \
                   websockets.connect(audio_uri) as audio_ws:
            
            print("✅ Connected! Start speaking... (Press Ctrl+C to stop)")
            
            async def send_audio():
                chunks_sent = 0
                try:
                    while True:
                        # Read audio from microphone (non-blocking asyncio thread)
                        data = await asyncio.to_thread(stream.read, CHUNK, exception_on_overflow=False)
                        await audio_ws.send(data)
                        chunks_sent += 1
                        
                        if debug and chunks_sent % 100 == 0:
                            print(f"[Debug] Sent {chunks_sent} chunks ({chunks_sent * 32}ms)...")
                        
                        # Yield to the event loop
                        await asyncio.sleep(0.001)
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    print(f"\n❌ Error sending audio: {e}")
                    
            async def receive_transcription():
                try:
                    while True:
                        response = await events_ws.recv()
                        data = json.loads(response)
                        
                        if debug:
                            print(f"\n[Debug server said]: {data}")

                        # Print VAD status and transcribed text
                        if "vad" in data:
                            if data["vad"] == "speech_start":
                                sys.stdout.write("\r🎤 Speech started...           ")
                                sys.stdout.flush()
                            elif data["vad"] == "speech_end":
                                sys.stdout.write("\r🛑 Speech ended.               ")
                                sys.stdout.flush()

                        if "text" in data and data["text"].strip():
                            # Clear the status line before printing the text
                            sys.stdout.write("\r" + " " * 30 + "\r")
                            print(f"[You]: {data['text']}")
                            
                except websockets.exceptions.ConnectionClosed as e:
                    print(f"\nEvents connection closed by server: {e}")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    print(f"\n❌ Error receiving transcription: {e}")
            
            # Run both send and receive tasks concurrently
            tasks = [
                asyncio.create_task(send_audio()),
                asyncio.create_task(receive_transcription())
            ]
            
            # Wait for either task to finish or fail
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            
            # Cancel any pending tasks
            for task in pending:
                task.cancel()
                
    except ConnectionRefusedError:
        print(f"❌ Could not connect to localhost:5092. Make sure the server is running!")
    except Exception as e:
        print(f"❌ Unexpected WebSocket error: {e}")
    finally:
        print("\nCleaning up audio stream...")
        try:
            stream.stop_stream()
            stream.close()
            p.terminate()
        except:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Print debug messages for chunks sent and raw server JSON")
    args = parser.parse_args()
    
    try:
        asyncio.run(audio_handler(debug=args.debug))
    except KeyboardInterrupt:
        import os
        print("\nClient stopped by user.")
        os._exit(0)
