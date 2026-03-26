"""
Voice Channel — real-time voice interface using FastRTC + Groq.

Refactored from src/fastrtc_data_stream.py to use the new enterprise
orchestrator. Now includes:
- Per-caller sessions
- Filler messages during slow queries
- Auth flow integration
- Business rule evaluation
"""

import asyncio
import argparse
import sys
from pathlib import Path
from typing import Generator, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
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
from app.orchestrator.agent_builder import get_agent_builder

logger.remove()
logger.add(
    lambda msg: print(msg),
    colorize=True,
    format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
)

groq_client = Groq()

# Initialize the enterprise agent builder
agent_builder = get_agent_builder()

# Track session per stream (in production, use proper session mapping)
_current_session = None


def _get_or_create_session():
    """Get current session or create a new one."""
    global _current_session
    if _current_session is None:
        _current_session = agent_builder.create_session(channel="voice")
        logger.info(f"📞 New voice session: {_current_session.id}")
    return _current_session


def _generate_tts(text: str):
    """Generate TTS audio from text and yield chunks."""
    from src.process_groq_tts import process_groq_tts

    agent_cfg = agent_builder.config.get("agent", {})
    tts_model = agent_cfg.get("tts_model", settings.GROQ_TTS_MODEL)
    tts_voice = agent_cfg.get("tts_voice", settings.GROQ_TTS_VOICE)

    tts_response = groq_client.audio.speech.create(
        model=tts_model,
        voice=tts_voice,
        response_format="wav",
        input=text,
    )
    yield from process_groq_tts(tts_response)


def _generate_filler():
    """Generate a filler message while the agent is thinking."""
    import random
    perf_cfg = agent_builder.config.get("performance", {})
    fillers = perf_cfg.get("filler_messages", [
        "Let me look that up for you...",
        "One moment while I check...",
        "Give me just a second...",
    ])
    filler_text = random.choice(fillers)
    logger.info(f"⏳ Filler: {filler_text}")
    return filler_text


def response(
    audio: tuple[int, np.ndarray],
) -> Generator[Tuple[int, np.ndarray], None, None]:
    """
    Process audio input through the enterprise AI agent pipeline.

    Flow:
        1. Groq Whisper STT → transcribe user speech
        2. Enterprise Agent → auth + rules + tool calling + response
        3. Filler messages if agent takes > 1.5s
        4. Groq TTS → convert response text to speech
        5. Yield audio chunks for FastRTC playback
    """
    session = _get_or_create_session()
    logger.info("🎙️ Received audio input (session: %s)", session.id[:8])

    # ── 1. Transcribe ────────────────────────────────────────────
    agent_cfg = agent_builder.config.get("agent", {})
    stt_model = agent_cfg.get("stt_model", settings.GROQ_STT_MODEL)

    transcript = groq_client.audio.transcriptions.create(
        file=("audio-file.mp3", audio_to_bytes(audio)),
        model=stt_model,
        response_format="text",
    )
    logger.info(f'👂 Transcribed: "{transcript}"')

    # ── 2. Invoke agent (with filler timeout) ────────────────────
    perf_cfg = agent_builder.config.get("performance", {})
    filler_timeout = perf_cfg.get("filler_timeout_seconds", 1.5)

    # Run agent in a thread since LangGraph is sync-heavy
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(
            asyncio.run,
            agent_builder.invoke(session, transcript),
        )

        try:
            response_text = future.result(timeout=filler_timeout)
        except concurrent.futures.TimeoutError:
            # Yield filler audio while waiting
            filler = _generate_filler()
            yield from _generate_tts(filler)

            # Now wait for the actual response
            response_text = future.result(timeout=30)

    logger.info(f'💬 Response: "{response_text}"')

    # ── 3. TTS ───────────────────────────────────────────────────
    yield from _generate_tts(response_text)


def create_stream() -> Stream:
    """Create and configure a FastRTC stream."""
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
    parser = argparse.ArgumentParser(
        description="Enterprise AI Agent — Voice Interface"
    )
    parser.add_argument(
        "--phone",
        action="store_true",
        help="Launch with FastRTC phone interface",
    )
    args = parser.parse_args()

    # Show startup info
    company = agent_builder.config.get("company", {}).get("name", "Unknown Company")
    adapters = agent_builder.registry.get_adapter_names() if agent_builder.registry else []
    auth_required = agent_builder.auth_manager.is_required if agent_builder.auth_manager else False

    logger.info(f"🏢 Company: {company}")
    logger.info(f"📊 Adapters: {adapters}")
    logger.info(f"🔐 Auth required: {auth_required}")

    stream = create_stream()
    logger.info("🎧 Stream handler configured")

    if args.phone:
        logger.info("📞 Launching with phone interface...")
        stream.fastphone()
    else:
        logger.info("🌐 Launching with Gradio UI...")
        stream.ui.launch()
