"""
OpenAI API client for RAVEN with model routing and cost tracking.

Features:
- Supports both Azure OpenAI and direct OpenAI API
- Automatic model selection per pipeline stage via config/model_routing.yaml
- Retry with exponential backoff on rate-limit and timeout errors
- Per-call cost tracking (input/output tokens × pricing)
- Embedding support (text-embedding-3-large via Azure 'embedlarge' deployment, 3072-dim)
- Async-first (uses openai.AsyncAzureOpenAI or openai.AsyncOpenAI)
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any

import structlog
import yaml
from openai import (
    AsyncAzureOpenAI,
    AsyncOpenAI,
    RateLimitError,
    APITimeoutError,
    APIError,
)

logger = structlog.get_logger(__name__)

# Pricing per 1M tokens (as of March 2026)
_PRICING: dict[str, dict[str, float]] = {
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt4o": {"input": 2.50, "output": 10.00},  # Azure deployment name
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt4o-mini": {"input": 0.15, "output": 0.60},  # Azure deployment name
    "text-embedding-3-small": {"input": 0.02, "output": 0.0},
    "text-embedding-3-large": {"input": 0.13, "output": 0.0},
}

_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "model_routing.yaml"

# Maximum retry attempts for transient errors
_MAX_RETRIES = 3
_BACKOFF_BASE = 1  # seconds


class OpenAIClient:
    """Async OpenAI client with per-stage model routing and cost tracking.

    Supports Azure OpenAI when AZURE_OPENAI_API_KEY and AZURE_OPENAI_API_BASE
    env vars are set.  Falls back to direct OpenAI API otherwise.
    """

    def __init__(
        self,
        api_key: str | None = None,
        config_path: Path | None = None,
    ) -> None:
        self._config = self._load_config(config_path or _CONFIG_PATH)
        self._cost_log: list[dict[str, Any]] = []
        self._unavailable_models: set[str] = set()  # track models that 404

        # Detect Azure vs direct OpenAI
        azure_key = api_key or os.getenv("AZURE_OPENAI_API_KEY", "")
        azure_base = os.getenv("AZURE_OPENAI_API_BASE", "")
        azure_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-03-01-preview")

        if azure_key and azure_base:
            self._client = AsyncAzureOpenAI(
                api_key=azure_key,
                azure_endpoint=azure_base,
                api_version=azure_version,
            )
            self._provider = "azure"
            logger.info(
                "openai_client_init",
                provider="azure",
                endpoint=azure_base[:60],
                api_version=azure_version,
            )

            # Separate Azure OpenAI client for embeddings (may use a different endpoint/deployment)
            embed_endpoint = os.getenv("AZURE_OPENAI_EMBED_ENDPOINT", "")
            embed_key = os.getenv("AZURE_OPENAI_EMBED_KEY", "")
            embed_version = os.getenv("AZURE_OPENAI_EMBED_API_VERSION", "2023-05-15")
            if embed_endpoint and embed_key:
                self._embed_client = AsyncAzureOpenAI(
                    api_key=embed_key,
                    azure_endpoint=embed_endpoint,
                    api_version=embed_version,
                )
                self._embed_deployment = os.getenv("AZURE_OPENAI_EMBED_DEPLOYMENT", "text-embedding-3-small")
                logger.info("openai_embed_client_init", endpoint=embed_endpoint[:60], deployment=self._embed_deployment)
            else:
                self._embed_client = self._client
                self._embed_deployment = None
        else:
            # Fallback to direct OpenAI
            direct_key = api_key or os.getenv("OPENAI_API_KEY", "")
            self._client = AsyncOpenAI(api_key=direct_key)
            self._embed_client = self._client
            self._embed_deployment = None
            self._provider = "openai"
            logger.info("openai_client_init", provider="openai")

    # ------------------------------------------------------------------ #
    # Config
    # ------------------------------------------------------------------ #

    @staticmethod
    def _load_config(path: Path) -> dict[str, Any]:
        """Load model_routing.yaml."""
        if path.exists():
            with open(path) as f:
                return yaml.safe_load(f) or {}
        logger.warning("model_routing_config_not_found", path=str(path))
        return {}

    def _stage_config(self, stage_name: str) -> dict[str, Any]:
        """Resolve config for a pipeline stage.  Supports nested keys like ``schema_selector.column_filter``.

        Handles model fallback: if the configured ``model`` deployment is not
        available (tracked in ``self._unavailable_models``), the stage-level
        ``fallback_model`` or the global ``fallback_model`` is used instead.
        """
        stages = self._config.get("stages", {})
        parts = stage_name.split(".")
        cfg = stages
        for part in parts:
            if isinstance(cfg, dict):
                cfg = cfg.get(part, {})
            else:
                break
        if not isinstance(cfg, dict) or "model" not in cfg:
            # Sensible default — use the primary deployment name (gpt4o for Azure)
            default_model = self._deployment or "gpt4o"
            return {"model": default_model, "max_tokens": 1000, "temperature": 0}

        # Model fallback: if configured model is known-unavailable, swap to fallback
        model = cfg["model"]
        if model in self._unavailable_models:
            fallback = (
                cfg.get("fallback_model")
                or self._config.get("fallback_model")
                or self._deployment
                or "gpt4o"
            )
            cfg = dict(cfg)  # copy to avoid mutating cached config
            cfg["model"] = fallback
        return cfg

    # ------------------------------------------------------------------ #
    # Chat completion
    # ------------------------------------------------------------------ #

    async def complete(
        self,
        prompt: str,
        stage_name: str,
        system_prompt: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        """Send a chat completion request routed to the correct model for *stage_name*.

        Parameters
        ----------
        prompt:
            User message content.
        stage_name:
            Pipeline stage key (e.g. ``"router"``, ``"schema_selector.column_filter"``).
        system_prompt:
            Optional system message.
        temperature:
            Override config temperature.
        max_tokens:
            Override config max_tokens.

        Returns
        -------
        str
            The assistant's response text.
        """
        cfg = self._stage_config(stage_name)
        model = cfg["model"]
        temp = temperature if temperature is not None else cfg.get("temperature", 0)
        mtok = max_tokens if max_tokens is not None else cfg.get("max_tokens", 1000)

        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                start = time.perf_counter()
                response = await self._client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=temp,
                    max_tokens=mtok,
                )
                elapsed_ms = (time.perf_counter() - start) * 1000

                usage = response.usage
                input_tokens = usage.prompt_tokens if usage else 0
                output_tokens = usage.completion_tokens if usage else 0
                cost = self._calculate_cost(model, input_tokens, output_tokens)

                self._cost_log.append(
                    {
                        "stage": stage_name,
                        "model": model,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "cost_usd": cost,
                        "latency_ms": round(elapsed_ms, 1),
                    }
                )
                logger.info(
                    "openai_complete",
                    stage=stage_name,
                    model=model,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cost_usd=round(cost, 6),
                    latency_ms=round(elapsed_ms, 1),
                )
                return response.choices[0].message.content or ""

            except (RateLimitError, APITimeoutError) as exc:
                wait = _BACKOFF_BASE * (2 ** (attempt - 1))
                logger.warning(
                    "openai_retry",
                    attempt=attempt,
                    wait_seconds=wait,
                    error=str(exc),
                    stage=stage_name,
                )
                await asyncio.sleep(wait)
            except APIError as exc:
                # Detect 404 (deployment not found) → mark model unavailable & retry w/ fallback
                if getattr(exc, "status_code", None) == 404 and model not in self._unavailable_models:
                    self._unavailable_models.add(model)
                    fallback = (
                        cfg.get("fallback_model")
                        or self._config.get("fallback_model")
                        or self._deployment
                        or "gpt4o"
                    )
                    logger.warning(
                        "openai_model_fallback",
                        missing_model=model,
                        fallback=fallback,
                        stage=stage_name,
                    )
                    model = fallback
                    continue  # retry immediately with fallback model
                logger.error("openai_api_error", error=str(exc), stage=stage_name)
                raise

        raise RuntimeError(f"OpenAI request failed after {_MAX_RETRIES} retries for stage '{stage_name}'")

    # ------------------------------------------------------------------ #
    # Embeddings
    # ------------------------------------------------------------------ #

    async def embed(self, text: str) -> list[float]:
        """Embed a single text string using the configured embedding model."""
        result = await self.batch_embed([text])
        return result[0]

    async def batch_embed(self, texts: list[str], batch_size: int = 2048) -> list[list[float]]:
        """Embed multiple texts in batches.

        Returns a list of embedding vectors.
        Uses the dedicated embedding client/deployment if configured.
        """
        # Use dedicated embed client if available
        embed_client = getattr(self, "_embed_client", self._client)
        embed_deployment = getattr(self, "_embed_deployment", None)

        emb_config = self._config.get("embeddings", {})
        model = embed_deployment or emb_config.get("model", "text-embedding-3-small")
        all_embeddings: list[list[float]] = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    start = time.perf_counter()
                    response = await embed_client.embeddings.create(
                        model=model,
                        input=batch,
                    )
                    elapsed_ms = (time.perf_counter() - start) * 1000

                    total_tokens = response.usage.total_tokens if response.usage else 0
                    cost = self._calculate_cost(model, total_tokens, 0)

                    self._cost_log.append(
                        {
                            "stage": "embedding",
                            "model": model,
                            "input_tokens": total_tokens,
                            "output_tokens": 0,
                            "cost_usd": cost,
                            "latency_ms": round(elapsed_ms, 1),
                        }
                    )
                    embeddings = [item.embedding for item in response.data]
                    all_embeddings.extend(embeddings)
                    break

                except (RateLimitError, APITimeoutError) as exc:
                    wait = _BACKOFF_BASE * (2 ** (attempt - 1))
                    logger.warning("openai_embed_retry", attempt=attempt, wait_seconds=wait, error=str(exc))
                    await asyncio.sleep(wait)
            else:
                raise RuntimeError(f"Embedding request failed after {_MAX_RETRIES} retries")

        return all_embeddings

    # ------------------------------------------------------------------ #
    # Cost tracking
    # ------------------------------------------------------------------ #

    @staticmethod
    def _calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost in USD from token counts."""
        pricing = _PRICING.get(model, {"input": 0.15, "output": 0.60})
        return (input_tokens * pricing["input"] + output_tokens * pricing["output"]) / 1_000_000

    def get_cost_summary(self) -> dict[str, Any]:
        """Aggregate costs by stage and model."""
        by_stage: dict[str, float] = {}
        by_model: dict[str, float] = {}
        total = 0.0
        for entry in self._cost_log:
            stage = entry["stage"]
            model = entry["model"]
            cost = entry["cost_usd"]
            by_stage[stage] = by_stage.get(stage, 0) + cost
            by_model[model] = by_model.get(model, 0) + cost
            total += cost
        return {
            "total_usd": round(total, 6),
            "by_stage": {k: round(v, 6) for k, v in by_stage.items()},
            "by_model": {k: round(v, 6) for k, v in by_model.items()},
            "call_count": len(self._cost_log),
        }

    def reset_cost_log(self) -> None:
        """Clear the accumulated cost log."""
        self._cost_log.clear()
