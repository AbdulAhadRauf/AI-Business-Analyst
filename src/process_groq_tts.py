"""Process Groq TTS response into audio segments for FastRTC."""

import os, tempfile, wave
from typing import Any, Generator, Tuple

import numpy as np


def process_groq_tts(tts_response: Any) -> Generator[Tuple[int, np.ndarray], None, None]:
    """Read Groq TTS WAV response and yield (sample_rate, audio_array) for FastRTC."""
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()
    try:
        tts_response.write_to_file(tmp.name)
        with wave.open(tmp.name, "rb") as wf:
            sample_rate = wf.getframerate()
            audio_data = wf.readframes(wf.getnframes())
        audio_array = np.frombuffer(audio_data, dtype=np.int16).reshape(1, -1)
        yield (sample_rate, audio_array)
    finally:
        if os.path.exists(tmp.name):
            os.remove(tmp.name)
