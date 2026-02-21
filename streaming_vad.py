from __future__ import annotations
import io, wave, tempfile, numpy as np, torch
from typing import List, Tuple
from torch.hub import load as torch_hub_load

vad_model, vad_utils = torch_hub_load("snakers4/silero-vad", "silero_vad")
(_, _, _, VADIterator, _) = vad_utils

# TODO: Update to read from .env
SAMPLE_RATE              = 16_000         # model is trained for 16 kHz
WINDOW_SAMPLES           = 512            # 32 ms frame
THRESHOLD                = 0.60           # voice prob ≥ 0.60 → speech
MIN_SILENCE_MS           = 250            # flush after ≥250 ms quiet
SPEECH_PAD_MS            = 120            # keep 120 ms context before/after
MAX_SPEECH_MS            = 8_000          # hard stop at 8 s

# Helper: float32 → int16 PCM bytes
def _f32_to_pcm16(frames: np.ndarray) -> bytes:
    return np.clip(frames * 32768, -32768, 32767).astype(np.int16).tobytes()

class StreamingVAD:
    """
    Feed successive 20–40 ms PCM frames (16 kHz, int16 mono).
    Emits temp-file *paths* when a full utterance is detected.
    """

    def __init__(self):
        self.vad = VADIterator(
            vad_model,
            sampling_rate=SAMPLE_RATE,
            threshold=THRESHOLD,
            min_silence_duration_ms=MIN_SILENCE_MS,
            speech_pad_ms=SPEECH_PAD_MS,
        )
        self.buffer = bytearray()
        self.speech_ms = 0
        self.is_speaking = False
        self.ring_buffer = bytearray()
        self.ring_buffer_maxlen = int((SPEECH_PAD_MS / 1000.0) * SAMPLE_RATE * 2)


    def _flush(self) -> Tuple[List[str], List[dict]]:
        events: List[dict] = []
        if self.is_speaking:
            events.append({"vad": "speech_end"})

        if not self.buffer:
            return [], events
            
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".wav")
        with wave.open(tmp, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(SAMPLE_RATE)
            wf.writeframes(self.buffer)
        self.buffer.clear()
        self.speech_ms = 0
        self.vad.reset_states()
        self.is_speaking = False
        return [tmp.name], events

    def feed(self, frame_bytes: bytes) -> Tuple[List[str], List[dict]]:
        out_paths: List[str] = []
        out_events: List[dict] = []

        pcm_f32 = np.frombuffer(frame_bytes, np.int16).astype("float32") / 32768.0
        for start in range(0, len(pcm_f32), WINDOW_SAMPLES):
            window = pcm_f32[start:start + WINDOW_SAMPLES]
            if len(window) < WINDOW_SAMPLES:
                break  # wait for full 32 ms window

            voice_event = self.vad(window, return_seconds=False)
            pcm16_chunk = _f32_to_pcm16(window)

            if voice_event is not None and "start" in voice_event:
                if not self.is_speaking:
                    out_events.append({"vad": "speech_start"})
                self.is_speaking = True
                self.buffer.extend(self.ring_buffer)
                self.speech_ms += len(self.ring_buffer) // (SAMPLE_RATE * 2 // 1000)
                self.ring_buffer.clear()

            if self.is_speaking:
                self.buffer.extend(pcm16_chunk)
                self.speech_ms += 32
            else:
                self.ring_buffer.extend(pcm16_chunk)
                if len(self.ring_buffer) > self.ring_buffer_maxlen:
                    self.ring_buffer = self.ring_buffer[-self.ring_buffer_maxlen:]

            # Flush on trailing-silence event or max-length guard
            if voice_event is not None and "end" in voice_event:
                paths, events = self._flush()
                out_paths.extend(paths)
                out_events.extend(events)
            elif self.speech_ms >= MAX_SPEECH_MS:
                paths, events = self._flush()
                out_paths.extend(paths)
                out_events.extend(events)

        return out_paths, out_events
