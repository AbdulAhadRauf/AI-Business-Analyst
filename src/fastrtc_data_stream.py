"""
FastRTC Data Stream — Real-time voice interface for the Universal Data Connector.

Uses FastRTC for WebRTC audio streaming, Groq for STT/TTS,
and LangGraph for intelligent tool calling against business data.

FILLER MESSAGE SUPPORT:
    When the agent takes longer than the configured timeout (default 1.5s)
    to respond (e.g. Snowflake queries), a random filler phrase is
    spoken to keep the user engaged instead of dead silence.

    Filler messages are configured in business_config.yaml:
        performance:
          filler_messages:
            - "Let me look that up for you..."
            - "One moment while I check..."
          filler_timeout_seconds: 1.5

Usage:
    python fastrtc_data_stream.py          # Gradio UI (default)
    python fastrtc_data_stream.py --phone  # Phone interface
"""

import argparse
import random
import sys
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path
from typing import Generator, Tuple

# ── Ensure app/ package is importable from src/ ────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import yaml
from fastrtc import (
    AlgoOptions,
    ReplyOnPause,
    Stream,
    audio_to_bytes,
)
from groq import Groq
from loguru import logger
from dotenv import load_dotenv
load_dotenv()

from app.config import settings
from process_groq_tts import process_groq_tts
from data_connector_agent import agent, agent_config

logger.remove()
logger.add(
    lambda msg: print(msg),
    colorize=True,
    format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
)

groq_client = Groq()

# ── Load filler config from business_config.yaml ──────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_config_path = PROJECT_ROOT / "business_config.yaml"
_biz_config = {}
if _config_path.exists():
    with open(_config_path, encoding="utf-8") as f:
        _biz_config = yaml.safe_load(f) or {}

_perf = _biz_config.get("performance", {})
FILLER_MESSAGES = _perf.get("filler_messages", [
    "Let me look that up for you...",
    "One moment while I check...",
    "Give me just a second...",
])
FILLER_TIMEOUT = _perf.get("filler_timeout_seconds", 1.5)

logger.info("🎤 Filler messages: {} phrases, timeout: {}s", len(FILLER_MESSAGES), FILLER_TIMEOUT)

# ── Thread pool for background agent calls ─────────────────────────────

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="agent")


def _invoke_agent(transcript: str) -> str:
    """Run the LangGraph agent (blocking) — called from the thread pool."""
    agent_response = agent.invoke(
        {"messages": [{"role": "user", "content": transcript}]}, config=agent_config
    )
    return agent_response["messages"][-1].content


def response(
    audio: tuple[int, np.ndarray],
) -> Generator[Tuple[int, np.ndarray], None, None]:
    """
    Process audio input, transcribe, generate response, and deliver TTS audio.

    Flow:
        1. Groq Whisper STT → transcribe user speech
        2. Submit agent.invoke() to a background thread
        3. Wait up to FILLER_TIMEOUT seconds for the agent to finish
        4. If not finished → yield filler TTS audio ("Let me look that up...")
        5. Wait for agent to complete
        6. Yield the real response TTS audio

    This means the user NEVER hears dead silence longer than FILLER_TIMEOUT,
    even when Snowflake or another slow data source is being queried.

    Args:
        audio: Tuple containing sample rate and audio data

    Yields:
        Tuples of (sample_rate, audio_array) for audio playback
    """
    logger.info("🎙️ Received audio input")

    # ── Step 1: Transcribe ─────────────────────────────────────────
    logger.debug("🔄 Transcribing audio...")
    transcript = groq_client.audio.transcriptions.create(
        file=("audio-file.mp3", audio_to_bytes(audio)),
        model=settings.GROQ_STT_MODEL,
        response_format="text",
    )
    logger.info(f'👂 Transcribed: "{transcript}"')

    # ── Step 2: Submit agent to background thread ──────────────────
    logger.debug("🧠 Running data connector agent (background)...")
    future: Future = _executor.submit(_invoke_agent, transcript)

    # ── Step 3: Wait with timeout ──────────────────────────────────
    try:
        response_text = future.result(timeout=FILLER_TIMEOUT)
        # Agent finished quickly — no filler needed
        logger.info(f'💬 Response (fast): "{response_text}"')
    except Exception:
        # ── Step 4: Agent still running — yield filler audio ───────
        filler = random.choice(FILLER_MESSAGES)
        logger.info(f'⏳ Agent still working — playing filler: "{filler}"')

        filler_tts = groq_client.audio.speech.create(
            model=settings.GROQ_TTS_MODEL,
            voice=settings.GROQ_TTS_VOICE,
            response_format="wav",
            input=filler,
        )
        yield from process_groq_tts(filler_tts)

        # ── Step 5: Now wait for the real response ─────────────────
        response_text = future.result()  # block until done
        logger.info(f'💬 Response (after filler): "{response_text}"')

    # ── Step 6: Yield real response audio ──────────────────────────
    logger.debug("🔊 Generating speech...")
    tts_response = groq_client.audio.speech.create(
        model=settings.GROQ_TTS_MODEL,
        voice=settings.GROQ_TTS_VOICE,
        response_format="wav",
        input=response_text,
    )
    yield from process_groq_tts(tts_response)


def create_stream() -> Stream:
    """
    Create and configure a Stream instance with audio capabilities.

    Returns:
        Stream: Configured FastRTC Stream instance
    """
    return Stream(
        modality="audio",
        mode="send-receive",
        handler=ReplyOnPause(
            response,
            algo_options=AlgoOptions(
                speech_threshold=0.5,
            ),
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FastRTC Universal Data Connector Voice Agent")
    parser.add_argument(
        "--phone",
        action="store_true",
        help="Launch with FastRTC phone interface (get a temp phone number)",
    )
    args = parser.parse_args()

    stream = create_stream()
    logger.info("🎧 Stream handler configured")

    if args.phone:
        logger.info("🌈 Launching with FastRTC phone interface...")
        stream.fastphone()
    else:
        logger.info("🌈 Launching with Gradio UI...")
        stream.ui.launch()
